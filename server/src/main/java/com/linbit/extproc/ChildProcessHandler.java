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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
        ioProgressMode = ioProgressModeRef;
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
            throw new ChildProcessTimeoutException();
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
        long lastIoBytes = -1;
        long stallStartMs = System.currentTimeMillis();
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
                throw new ChildProcessTimeoutException();
            }

            long currentIoBytes = readProcIoBytes(childProcess.pid());
            long now = System.currentTimeMillis();

            if (currentIoBytes < 0 || currentIoBytes != lastIoBytes)
            {
                // I/O progress detected (or /proc unreadable — assume progress)
                lastIoBytes = currentIoBytes;
                stallStartMs = now;
            }
            else
            if (now - stallStartMs >= ioStallTimeout)
            {
                throw new ChildProcessTimeoutException();
            }
        }
        return exitCode;
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
