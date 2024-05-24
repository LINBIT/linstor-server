package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.OutputProxy.EOFEvent;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;
import com.linbit.linstor.logging.ErrorReporter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BackupShippingL2LDaemon implements Runnable, BackupShippingDaemon
{
    private static final String FIRST_SOCAT_CONNECT_MSG = "accepting connection";
    private static final int DFLT_DEQUE_CAPACITY = 100;
    private static final String ALREADY_IN_USE = "Address already in use";
    private static final Pattern SOCAT_LOG_NOTICE = Pattern.compile(
        "\\d{4}(?:[/|:| ]\\d{2}){5} socat\\[\\d+\\] N (.*)"
    );

    private final Object syncObj = new Object();
    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final LinkedBlockingDeque<Boolean> waitForConnDeque;
    private final Integer timeoutInMs;
    private final String[] command;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;

    private final BiConsumer<Boolean, Integer> afterTermination;
    private final Integer port;

    private boolean started = false;
    private boolean shutdown = false;
    private boolean alreadyInUse = false;
    private boolean prepareAbort = false;
    private boolean runAfterTermination = false;

    public BackupShippingL2LDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        Integer portRef,
        BiConsumer<Boolean, Integer> postActionRef,
        @Nullable Integer timeoutInMsRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;
        afterTermination = postActionRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);

        port = portRef;

        thread = new Thread(threadGroupRef, this, threadName);
        timeoutInMs = timeoutInMsRef;
        if (timeoutInMsRef != null)
        {
            waitForConnDeque = new LinkedBlockingDeque<>(1);
        }
        else
        {
            waitForConnDeque = null;
        }
    }

    /**
     * Interrupts the daemon after a given timeout unless the thread is cancelled by adding <code>true</code> into the
     * {@link BackupShippingL2LDaemon#waitForConnDeque}
     */
    private void waitForConn()
    {
        final long waitUntil = System.currentTimeMillis() + timeoutInMs;
        while (started && System.currentTimeMillis() < waitUntil)
        {
            try
            {
                @Nullable Boolean pollResult = waitForConnDeque.poll(
                    waitUntil - System.currentTimeMillis(),
                    TimeUnit.MILLISECONDS
                );
                if ((pollResult == null || !pollResult) && started)
                {
                    errorReporter.logWarning(
                        "Stopped waiting for shipment, either due to an error or " +
                            "because the source-cluster did not connect within %d ms",
                        timeoutInMs
                    );
                    shutdown(true);
                }
                break;
            }
            catch (InterruptedException exc)
            {
                if (started)
                {
                    reportError(new ImplementationError(exc));
                }
            }
        }
    }

    private void reportError(Throwable exc)
    {
        if (!prepareAbort)
        {
            errorReporter.reportError(exc);
        }
        else
        {
            errorReporter.logDebug("Waiting for abort, ignoring error %s", exc.getMessage());
        }
    }

    @Override
    public String start()
    {
        synchronized (syncObj)
        {
            started = true;
            thread.start();
            if (timeoutInMs != null)
            {
                new Thread(thread.getThreadGroup(), this::waitForConn, "waitForConn" + thread.getName())
                    .start();
            }
            try
            {
                handler.startDelimited();
            }
            catch (IOException exc)
            {
                reportError(
                    new SystemServiceStartException(
                        "Unable to daemon for SnapshotShipping",
                        "I/O error attempting to start '" + Arrays.toString(command) + "'",
                        exc.getMessage(),
                        null,
                        null,
                        exc,
                        false
                    )
                );
                shutdown(false);
            }
        }
        return null;
    }

    @Override
    public void run()
    {
        errorReporter.logTrace("starting daemon: %s", Arrays.toString(command));
        boolean first = true;
        boolean afterTerminationCalled = false;
        while (started)
        {
            Event event;
            try
            {
                event = deque.take();
                if (event instanceof StdOutEvent)
                {
                    errorReporter.logTrace("stdOut: %s", new String(((StdOutEvent) event).data));
                    // ignore for now...
                }
                else
                if (event instanceof StdErrEvent)
                {
                    String stdErr = new String(((StdErrEvent) event).data);
                    Matcher mLog = SOCAT_LOG_NOTICE.matcher(stdErr);
                    if (!mLog.matches())
                    {
                        errorReporter.logWarning("stdErr: %s", stdErr);
                    }
                    else if (timeoutInMs != null && mLog.group(1).contains(FIRST_SOCAT_CONNECT_MSG))
                    {
                        waitForConnDeque.offer(true);
                    }
                    if (stdErr.contains(ALREADY_IN_USE))
                    {
                        alreadyInUse = true;
                    }
                }
                else
                if (event instanceof ExceptionEvent)
                {
                    errorReporter.logTrace("ExceptionEvent in '%s':", Arrays.toString(command));
                    reportError(((ExceptionEvent) event).exc);
                    // FIXME: Report the exception to the controller
                }
                else
                if (event instanceof PoisonEvent)
                {
                    errorReporter.logTrace("PoisonEvent");
                    break;
                }
                else
                if (event instanceof EOFEvent)
                {
                    if (!first)
                    {
                        int exitCode = handler.getExitCode();
                        errorReporter.logTrace(
                            "EOF. Exit code: %d %s",
                            exitCode,
                            (alreadyInUse && exitCode == 0 ? " but port is already in use" : "")
                        );
                        afterTermination.accept(exitCode == 0 && !alreadyInUse, alreadyInUse ? port : null);
                        afterTerminationCalled = true;
                    }
                    else
                    {
                        first = false;
                    }
                }
            }
            catch (InterruptedException exc)
            {
                if (started)
                {
                    reportError(exc);
                }
            }
            catch (Exception exc)
            {
                reportError(
                    new ImplementationError(
                        "Unknown exception occurred while executing '" + Arrays.toString(command) + "'",
                        exc
                    )
                );
            }
        }
        if (timeoutInMs != null)
        {
            // just in case this wasn't stopped yet
            waitForConnDeque.offer(false);
        }
        if (runAfterTermination && !afterTerminationCalled)
        {
            afterTermination.accept(false, null);
        }
    }

    /**
     * Simple event forcing this thread to kill itself
     */
    private static class PoisonEvent implements Event
    {
    }

    @Override
    public void shutdown(boolean runAfterTerminationRef)
    {
        runAfterTermination = runAfterTerminationRef;
        started = false;
        shutdown = true;
        handler.stop(false);
        if (thread != null)
        {
            thread.interrupt();
        }
        deque.addFirst(new PoisonEvent());
        if (timeoutInMs != null)
        {
            waitForConnDeque.offer(false);
        }
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (thread != null)
        {
            thread.join(timeoutRef);
        }
    }

    @Override
    public void setPrepareAbort()
    {
        synchronized (syncObj)
        {
            if (!shutdown)
            {
                if (!started)
                {
                    afterTermination.accept(false, null);
                }
                else
                {
                    prepareAbort = true;
                }
            }
        }
    }
}
