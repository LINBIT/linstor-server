package com.linbit.extproc;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.NegativeTimeException;
import com.linbit.Platform;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;
import com.linbit.utils.StringUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Process spawner &amp; handler for running external processes
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ChildProcessHandler
{
    /** Default: Wait up to 45 seconds for a child process to exit */
    private static volatile long dfltWaitTimeout = 45000;
    /** Default: Wait up to 15 seconds for a child process to exit after receiving a signal */
    private static volatile long dfltTermTimeout = 15000;
    /** Default: Wait up to 5 seconds for a child process to exit after the operating system has been ordered to
      * enforce termination of the process */
    private static volatile long dfltKillTimeout = 5000;
    /** Default: I/O stall timeout inherits from dfltWaitTimeout when null */
    private static volatile @Nullable Long dfltIoStallTimeout = null;
    /** Default: Wait for 2 seconds between polling /proc/&lt;pid>/io*/
    private static volatile long dfltIoAwarePollInterval = 2_000;

    // field indices of /sys/block/<dev>/stat, see Documentation/block/stat.rst
    private static final int STAT_IDX_SECTORS_READ = 2;
    private static final int STAT_IDX_SECTORS_WRITTEN = 6;
    private static final int STAT_IDX_SECTORS_DISCARDED = 13;

    public enum TimeoutType
    {
        WAIT,
        TERM,
        KILL,
        IO_STALL,
        IO_POLL
    }

    private long waitTimeout = dfltWaitTimeout;
    private long termTimeout = dfltTermTimeout;
    private long killTimeout = dfltKillTimeout;

    private boolean autoTerm = true;
    private boolean autoKill = true;

    private boolean ioProgressMode = false;
    private long ioStallTimeout = getEffectiveIoStallTimeout();
    private long ioPollInterval = dfltIoAwarePollInterval;
    /** Resolved sysfs 'stat' files (e.g. /sys/block/dm-5/stat) to additionally watch, or empty */
    private Set<String> ioProgressDeviceStatFiles = Collections.emptySet();

    private final Timer<String, Action<String>> timeoutScheduler;
    private @Nullable Process childProcess;

    public ChildProcessHandler(Timer<String, Action<String>> timer)
    {
        childProcess = null;
        ErrorCheck.ctorNotNull(ChildProcessHandler.class, Timer.class, timer);
        timeoutScheduler = timer;
    }

    public ChildProcessHandler(Process child, Timer<String, Action<String>> timer)
    {
        this(timer);
        ErrorCheck.ctorNotNull(ChildProcessHandler.class, Process.class, child);
        childProcess = child;
    }

    public void setChild(Process child)
    {
        if (child == null)
        {
            throw new ImplementationError(
                ChildProcessHandler.class.getName() +
                ": method called with child == null",
                new NullPointerException()
            );
        }
        childProcess = child;
    }

    // SuppressWarnings because ErrorProne forces us to use new-syntax switch "case TERM -> ...;"
    // while checkstyle then complains about "InnerAssignment". Suppressing this warning satisfies both
    // checkstyle's "InnerAssignment" rule was supposed to catch things like 'if((i = read()) == -1)', which
    // admittedly is not so easy to read/understand. This switch here on the other hand is simple enough to
    // ignore this false positive warning.
    @SuppressWarnings("InnerAssignment")
    public void setTimeout(TimeoutType type, long timeout)
    {
        if (timeout < 0)
        {
            throw new ImplementationError(
                ChildProcessHandler.class.getName() +
                ": Bad timer value: timeout < 0",
                new NegativeTimeException()
            );
        }

        switch (type)
        {
            case TERM -> termTimeout = timeout;
            case KILL -> killTimeout = timeout;
            case WAIT -> waitTimeout = timeout;
            case IO_STALL -> ioStallTimeout = timeout;
            case IO_POLL -> ioPollInterval = timeout;
            default -> throw new ImplementationError("Unhandled case: " + type.name());
        }
    }

    public void setAutoTerm(boolean flag)
    {
        autoTerm = flag;
    }

    public void setAutoKill(boolean flag)
    {
        autoKill = flag;
    }

    /**
     * Enables or disables I/O progress monitoring mode. Instead of a fixed wall-clock timeout,
     * the process is monitored via /proc/&lt;pid&gt;/io. As long as read_bytes + write_bytes
     * keep changing, the process is considered active. Only if I/O stalls for
     * {@code stallTimeoutMs} will a timeout be raised.
     */
    public ChildProcessHandler setIoProgressMode(boolean ioProgressModeRef)
    {
        return setIoProgressMode(ioProgressModeRef, Collections.emptySet());
    }

    /**
     * Like {@link #setIoProgressMode(boolean)}, but additionally watches the given sysfs 'stat' files
     * (typically {@code /sys/block/<dev>/stat} or {@code /sys/dev/block/<maj>:<min>/stat}) for
     * block-layer activity.
     *
     * <p>This closes the blind spot of {@code /proc/<pid>/io} during the final flush/fsync phase:
     * {@code write_bytes} is accounted at page-<em>dirtying</em> time, not at writeback time. Once the
     * child has dirtied all its pages and only waits for the kernel writeback threads to flush them,
     * its per-process counters freeze even though the device is still busy. The device 'stat' counters
     * keep advancing during that phase, so OR-combining both signals covers the whole lifecycle
     * (dirtying via {@code /proc/<pid>/io}, flushing via the device 'stat' files).</p>
     *
     * <p><b>Caveat:</b> {@code /sys/.../stat} is device-<em>global</em> &mdash; it aggregates I/O from
     * <em>all</em> processes touching that device, so it is only a trustworthy liveness signal for
     * commands with effectively exclusive access to the monitored device(s) (e.g. mkfs on a fresh
     * volume, {@code drbdadm create-md} on the meta device). Do not rely on it for commands that share
     * the device with concurrent workloads; a genuine hang could be masked by unrelated I/O.</p>
     *
     * @param ioProgressModeRef enables/disables I/O progress monitoring
     * @param deviceStatFilesRef already-resolved sysfs 'stat' file paths to watch (resolution is
     *     caller-side, see satellite {@code DeviceStatUtils}); may be empty/null
     */
    public ChildProcessHandler setIoProgressMode(boolean ioProgressModeRef, Collection<String> deviceStatFilesRef)
    {
        ioProgressMode = ioProgressModeRef;
        ioProgressDeviceStatFiles = ioProgressModeRef && deviceStatFilesRef != null && !deviceStatFilesRef.isEmpty()
            ? Set.copyOf(deviceStatFilesRef)
            : Collections.emptySet();
        return this;
    }

    public int waitFor() throws ChildProcessTimeoutException
    {
        if (childProcess == null)
        {
            throw new ImplementationError(
                ChildProcessHandler.class.getName() +
                ": method called while childProcess == null",
                new NullPointerException()
            );
        }
        int exitCode = -1;
        try
        {
            if (ioProgressMode)
            {
                if (Platform.isLinux())
                {
                    exitCode = waitForWithIoProgress();
                }
                else
                {
                    // sorry, not (yet?) supported for Windows.
                    exitCode = waitFor(waitTimeout);
                }
            }
            else
            {
                exitCode = waitFor(waitTimeout);
            }
        }
        catch (ChildProcessTimeoutException waitTimeoutExc)
        {
            if (autoTerm)
            {
                try
                {
                    waitForDestroy();
                    waitTimeoutExc = new ChildProcessTimeoutException(true, waitTimeoutExc);
                }
                catch (ChildProcessTimeoutException termTimedOut)
                {
                    if (autoKill)
                    {
                        if (waitForDestroyForcibly())
                        {
                            waitTimeoutExc = new ChildProcessTimeoutException(true, waitTimeoutExc);
                        }
                    }
                }
            }
            throw waitTimeoutExc;
        }
        return exitCode;
    }

    private int waitFor(long timeout) throws ChildProcessTimeoutException
    {
        if (childProcess == null)
        {
            throw new ImplementationError(
                ChildProcessHandler.class.getName() +
                ": method called while childProcess == null",
                new NullPointerException()
            );
        }
        int exitCode = -1;
        // Measured rather than reporting the configured timeout: the two can differ (e.g. a
        // stray interrupt), and the measured value is the one worth having in an ErrorReport
        final long startedAtMs = System.currentTimeMillis();
        try
        {
            Interruptor intrAction = new Interruptor();
            try
            {
                timeoutScheduler.addDelayedAction(timeout, intrAction);
            }
            catch (NegativeTimeException | ValueOutOfRangeException implExc)
            {
                throw new ImplementationError("Bad timer value", implExc);
            }
            exitCode = childProcess.waitFor();

            // If the waitFor() ended without being interrupted, cancel the timeout
            // and cleanup the thread's interrupted status if the interrupt arrived
            // between the return from waitFor() and the cancellation of the timeout
            timeoutScheduler.cancelAction(intrAction.getId());
            // Cancel this thread's interrupted status
            Thread.interrupted();
        }
        catch (InterruptedException interrupted)
        {
            long waitedMs = System.currentTimeMillis() - startedAtMs;
            throw new ChildProcessTimeoutException(
                "The process did not exit within " + waitedMs + "ms (timeout: " + timeout + "ms)",
                waitedMs
            );
        }
        return exitCode;
    }

    public int waitForDestroy() throws ChildProcessTimeoutException
    {
        if (childProcess == null)
        {
            throw new ImplementationError(
                ChildProcessHandler.class.getName() +
                ": method called while childProcess == null",
                new NullPointerException()
            );
        }
        childProcess.destroy();
        return waitFor(termTimeout);
    }

    public boolean waitForDestroyForcibly()
    {
        if (childProcess == null)
        {
            throw new ImplementationError(
                ChildProcessHandler.class.getName() +
                ": method called while childProcess == null",
                new NullPointerException()
            );
        }
        boolean killed = false;
        childProcess.destroyForcibly();
        try
        {
            waitFor(killTimeout);
            killed = true;
        }
        catch (ChildProcessTimeoutException ignored)
        {
        }
        return killed;
    }

    private int waitForWithIoProgress() throws ChildProcessTimeoutException
    {
        long lastPidIoBytes = -1;
        long lastDeviceSectors = -1;
        final long startedAtMs = System.currentTimeMillis();
        long stallStartMs = startedAtMs;
        int exitCode = -1;

        while (true)
        {
            try
            {
                boolean exited = childProcess.waitFor(ioPollInterval, TimeUnit.MILLISECONDS);
                if (exited)
                {
                    exitCode = childProcess.exitValue();
                    break;
                }
            }
            catch (InterruptedException ignored)
            {
                Thread.currentThread().interrupt();
                long waitedMs = System.currentTimeMillis() - startedAtMs;
                throw new ChildProcessTimeoutException(
                    "Interrupted after waiting " + waitedMs + "ms for the process to exit",
                    waitedMs
                );
            }

            long now = System.currentTimeMillis();
            long pidIoBytes = readProcIoBytes(childProcess.pid());
            long deviceSectors = readDeviceStatSectors(ioProgressDeviceStatFiles);

            // OR-combine two independent progress signals so the whole command lifecycle is covered:
            //   - pidIoBytes    (/proc/<pid>/io): per-process, reflects the *dirtying* phase, but freezes
            //                    during the final writeback/fsync (write_bytes is charged at dirty time,
            //                    and the actual flush is done by kernel writeback threads, not this pid).
            //   - deviceSectors (/sys/.../stat): device-global, keeps advancing during that flush phase.
            // A negative value means "unreadable" and is optimistically treated as progress, matching the
            // existing readProcIoBytes semantics (e.g. the process exited between waitFor() and here).
            boolean progressed = hasIoProgress(pidIoBytes, lastPidIoBytes, deviceSectors, lastDeviceSectors);

            if (progressed)
            {
                lastPidIoBytes = pidIoBytes;
                lastDeviceSectors = deviceSectors;
                stallStartMs = now;
            }
            else
            if (now - stallStartMs >= ioStallTimeout)
            {
                throw new ChildProcessTimeoutException(
                    "No I/O progress within " + (now - stallStartMs) + "ms (I/O stall timeout: " +
                        ioStallTimeout + "ms), total runtime " + (now - startedAtMs) + "ms",
                    now - startedAtMs
                );
            }
        }
        return exitCode;
    }

    /**
     * Decides whether I/O progress was observed since the previous poll, OR-combining the
     * per-process {@code /proc/<pid>/io} signal with the device-global {@code /sys/.../stat} signal.
     *
     * <ul>
     *   <li>{@code pidIoBytes < 0}: {@code /proc/<pid>/io} unreadable (e.g. the process just exited) -
     *       optimistically counted as progress, preserving the pre-existing behavior.</li>
     *   <li>{@code pidIoBytes != lastPidIoBytes}: the process advanced its own counters (dirtying
     *       phase).</li>
     *   <li>{@code deviceSectors >= 0 && deviceSectors != lastDeviceSectors}: a monitored device
     *       advanced its sector counters (writeback/flush phase). A negative {@code deviceSectors}
     *       means "no device configured / none readable" and simply does not contribute - it never
     *       forces progress on its own, so the per-process signal still governs.</li>
     * </ul>
     */
    static boolean hasIoProgress(
        long pidIoBytes,
        long lastPidIoBytes,
        long deviceSectors,
        long lastDeviceSectors
    )
    {
        return pidIoBytes < 0 ||
            pidIoBytes != lastPidIoBytes ||
            (deviceSectors >= 0 && deviceSectors != lastDeviceSectors);
    }

    /**
     * Assumes the set of Strings all being {@code /sys/dev/block/<something>/stat} or
     * {@code /sys/block/<something>/stat} where reading the file and splitting by whitespace [2], [6] and [13]
     * gives the read/write/discard values.
     *
     * <p>Sums 'sectors read' + 'sectors written' + 'sectors discarded' across the given sysfs 'stat' files. Unlike
     * {@code /proc/<pid>/io} {@code write_bytes}, these device-level counters keep advancing during
     * buffered writeback, which is what lets I/O progress mode survive a long mkfs / create-md flush.</p>
     *
     * @return the summed sector count (arbitrary 512-byte units, only diffs matter), or -1 if no file
     *     was configured or none could be read.
     */
    private static long readDeviceStatSectors(Set<String> statFiles)
    {
        long ret = -1;
        if (Platform.isLinux() && !statFiles.isEmpty())
        {
            long total = 0;
            boolean anyReadable = false;
            for (String statFile : statFiles)
            {
                try
                {
                    List<String> lines = Files.readAllLines(Paths.get(statFile));
                    if (!lines.isEmpty())
                    {
                        // single whitespace-separated line; field index 2 = sectors read, index 6 = sectors
                        // written, index 13 = sectors discarded (kernel 4.18+, see Documentation/block/stat.rst)
                        String[] fields = StringUtils.split(lines.get(0).trim(), "\\s+");
                        if (fields.length > STAT_IDX_SECTORS_WRITTEN)
                        {
                            total += Long.parseLong(fields[STAT_IDX_SECTORS_READ]) +
                                Long.parseLong(fields[STAT_IDX_SECTORS_WRITTEN]);
                            if (fields.length > STAT_IDX_SECTORS_DISCARDED)
                            {
                                total += Long.parseLong(fields[STAT_IDX_SECTORS_DISCARDED]);
                            }
                            anyReadable = true;
                        }
                    }
                }
                catch (IOException | NumberFormatException ignored)
                {
                    // device vanished / unreadable -> skip this one
                }
            }
            if (anyReadable)
            {
                ret = total;
            }
        }
        return ret;
    }

    /**
     * Reads /proc/&lt;pid&gt;/io and returns the sum of read_bytes + write_bytes,
     * or -1 if the file cannot be read (non-Linux, permissions, process gone).
     */
    private static long readProcIoBytes(long pid)
    {
        long ret = -1;
        if (Platform.isLinux())
        {
            try
            {
                long total = 0;
                for (String line : Files.readAllLines(Paths.get("/proc/" + pid + "/io")))
                {
                    if (line.startsWith("read_bytes:") || line.startsWith("write_bytes:"))
                    {
                        total += Long.parseLong(line.substring(line.indexOf(':') + 1).trim());
                    }
                }
                ret = total;
            }
            catch (IOException | NumberFormatException ignored)
            {
                // NoSuchFileException (extends IOException) -> Process most likely already exited
                // in any case keep and return ret = -1
            }
        } // else (presumably Windows case): just return -1, since we do not have /proc/<pid>/io
        return ret;
    }

    private static long getEffectiveIoStallTimeout()
    {
        return dfltIoStallTimeout != null ? dfltIoStallTimeout : dfltWaitTimeout;
    }

    /**
     * Reads ext-cmd timeout properties from the given priority-resolved props and updates
     * the static default fields. Node props take precedence over controller (stlt) props.
     *
     * @param propsArr The ReadOnlyProps that should be used as prioProps (order is kept)
     */
    public static void applyTimeoutProps(ReadOnlyProps... propsArr)
    {
        PriorityProps prioProps = new PriorityProps(propsArr);

        // we try to parse all properties before applying them to achieve an "all or nothing" approach
        @Nullable Long waitTimeout = parseLong(prioProps, ApiConsts.KEY_WAIT_TO, ApiConsts.NAMESPC_EXT_CMD);
        @Nullable Long termTimeout = parseLong(prioProps, ApiConsts.KEY_TERM_TO, ApiConsts.NAMESPC_EXT_CMD);
        @Nullable Long killTimeout = parseLong(prioProps, ApiConsts.KEY_KILL_TO, ApiConsts.NAMESPC_EXT_CMD);
        @Nullable Long ioStallTimeout = parseLong(prioProps, ApiConsts.KEY_IO_STALL_TO, ApiConsts.NAMESPC_EXT_CMD);
        @Nullable Long ioPollInterval = parseLong(prioProps, ApiConsts.KEY_IO_POLL_INTERVAL, ApiConsts.NAMESPC_EXT_CMD);

        if (waitTimeout != null)
        {
            dfltWaitTimeout = waitTimeout;
        }
        if (termTimeout != null)
        {
            dfltTermTimeout = termTimeout;
        }
        if (killTimeout != null)
        {
            dfltKillTimeout = killTimeout;
        }

        // dfltIoStallTimeout can be null.
        // If it is null it will inherit the value from dfltWaitTimeout which must not be null
        dfltIoStallTimeout = ioStallTimeout;

        if (ioPollInterval != null)
        {
            dfltIoAwarePollInterval = ioPollInterval;
        }
    }

    private static @Nullable Long parseLong(PriorityProps prioPropsRef, String keyRef, String namespcRef)
    {
        @Nullable String value = prioPropsRef.getProp(keyRef, namespcRef);
        @Nullable Long ret;
        if (value == null)
        {
            ret = null;
        }
        else
        {
            try
            {
                ret = Long.parseLong(value);
            }
            catch (NumberFormatException nfe)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_PROP,
                        String.format(
                            "The property %s/%s has to have a numeric value. Current value: %s",
                            namespcRef,
                            keyRef,
                            value
                        )
                    ),
                    nfe
                );
            }
        }
        return ret;
    }

    private static class Interruptor implements Action<String>
    {
        private final Thread targetThread;
        private final Long targetThreadId;

        private Interruptor()
        {
            this(Thread.currentThread());
        }

        private Interruptor(Thread target)
        {
            targetThread = target;
            targetThreadId = target.getId();
        }

        @Override
        public void run()
        {
            targetThread.interrupt();
        }

        @Override
        public String getId()
        {
            return "INTR-" + Long.toString(targetThreadId);
        }
    }

    public static long getDefaultWaitTimeout()
    {
        return dfltWaitTimeout;
    }
}
