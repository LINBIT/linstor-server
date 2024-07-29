package com.linbit.extproc;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;
import com.linbit.NegativeTimeException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

/**
 * Process spawner &amp; handler for running external processes
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ChildProcessHandler
{
    // Default: Wait up to 45 seconds for a child process to exit
    public static long dfltWaitTimeout = 45000;

    // Default: Wait up to 15 seconds for a child process to exit
    //          after receiving a signal
    public static long dfltTermTimeout = 15000;

    // Default: Wait up to 5 seconds for a child process to exit
    //          after the operating system has been ordered to enforce
    //          termination of the process
    public static long dfltKillTimeout =  5000;

    public enum TimeoutType
    {
        WAIT,
        TERM,
        KILL
    }

    private long waitTimeout = dfltWaitTimeout;
    private long termTimeout = dfltTermTimeout;
    private long killTimeout = dfltKillTimeout;

    private boolean autoTerm = true;
    private boolean autoKill = true;

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
            case TERM:
                termTimeout = timeout;
                break;
            case KILL:
                killTimeout = timeout;
                break;
            case WAIT:
                // fall-through
            default:
                waitTimeout = timeout;
                break;
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
            exitCode = waitFor(waitTimeout);
        }
        catch (ChildProcessTimeoutException waitTimeoutExc)
        {
            if (autoTerm)
            {
                try
                {
                    waitForDestroy();
                    waitTimeoutExc = new ChildProcessTimeoutException(true);
                }
                catch (ChildProcessTimeoutException termTimedOut)
                {
                    if (autoKill)
                    {
                        if (waitForDestroyForcibly())
                        {
                            waitTimeoutExc = new ChildProcessTimeoutException(true);
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
}
