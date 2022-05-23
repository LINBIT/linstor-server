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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiConsumer;

public class BackupShippingL2LDaemon implements Runnable, BackupShippingDaemon
{
    private static final int DFLT_DEQUE_CAPACITY = 100;
    private static final String ALREADY_IN_USE = "Address already in use";

    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final String[] command;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;

    private final Integer port;

    private boolean started = false;
    private boolean alreadyInUse = false;

    private final BiConsumer<Boolean, Integer> afterTermination;
    private boolean runAfterTermination = false;

    public BackupShippingL2LDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        Integer portRef,
        BiConsumer<Boolean, Integer> postActionRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;
        afterTermination = postActionRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);

        port = portRef;

        thread = new Thread(threadGroupRef, this, threadName);
    }

    @Override
    public String start()
    {
        started = true;
        thread.start();
        try
        {
            handler.startDelimited();
        }
        catch (IOException exc)
        {
            errorReporter.reportError(
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
                    errorReporter.logWarning("stdErr: %s", stdErr);
                    if (stdErr.contains(ALREADY_IN_USE))
                    {
                        alreadyInUse = true;
                    }
                }
                else
                if (event instanceof ExceptionEvent)
                {
                    errorReporter.logTrace("ExceptionEvent in '%s':", Arrays.toString(command));
                    errorReporter.reportError(((ExceptionEvent) event).exc);
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
                    errorReporter.reportError(new ImplementationError(exc));
                }
            }
            catch (Exception exc)
            {
                errorReporter.reportError(
                    new ImplementationError(
                        "Unknown exception occurred while executing '" + Arrays.toString(command) + "'",
                        exc
                    )
                );
            }
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
        handler.stop(false);
        if (thread != null)
        {
            thread.interrupt();
        }
        deque.addFirst(new PoisonEvent());
    }

    @Override
    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (thread != null)
        {
            thread.join(timeoutRef);
        }
    }
}
