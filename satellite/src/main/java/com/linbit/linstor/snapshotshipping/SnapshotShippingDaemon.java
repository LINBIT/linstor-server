package com.linbit.linstor.snapshotshipping;

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

@Deprecated(forRemoval = true)
public class SnapshotShippingDaemon implements Runnable
{
    private static final int DFLT_DEQUE_CAPACITY = 100;
    private static final String ALREADY_IN_USE = "Address already in use";

    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final String[] command;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;

    private boolean started = false;
    private boolean alreadyInUse = false;

    private final BiConsumer<Boolean, Boolean> afterTermination;

    public SnapshotShippingDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        BiConsumer<Boolean, Boolean> afterTerminationRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;
        afterTermination = afterTerminationRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);

        thread = new Thread(threadGroupRef, this, threadName);
    }

    public void start()
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
            shutdown();
        }
    }

    @Override
    public void run()
    {
        errorReporter.logTrace("starting daemon: %s", Arrays.toString(command));
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
                    int exitCode = handler.getExitCode();
                    errorReporter.logTrace("EOF. Exit code: " + exitCode);
                    afterTermination.accept(exitCode == 0, alreadyInUse);
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
                        "An exception of type " + exc.getClass().getSimpleName() +
                        " occurred while executing '" + Arrays.toString(command) + "'",
                        exc
                    )
                );
            }
        }
    }

    /**
     * Simple event forcing this thread to kill itself
     */
    private static class PoisonEvent implements Event
    {
    }

    public void shutdown()
    {
        started = false;
        handler.stop(false);
        thread.interrupt();
        deque.addFirst(new PoisonEvent());
    }

    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        thread.join(timeoutRef);
    }
}
