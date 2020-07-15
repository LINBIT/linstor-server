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
import java.util.function.Consumer;

public class SnapshotShippingDaemon implements Runnable
{
    private static final int DFLT_DEQUE_CAPACITY = 100;

    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final String[] command;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;

    private boolean started = false;

    private final Consumer<Boolean> afterTermination;

    public SnapshotShippingDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        Consumer<Boolean> afterTerminationRef
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
        thread.start();
        errorReporter.logTrace("starting daemon: %s", Arrays.toString(command));
        try
        {
            handler.start();
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
        }
        started = true;
    }

    @Override
    public void run()
    {
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
                    errorReporter.logTrace("stdErr: %s", new String(((StdErrEvent) event).data));
                    errorReporter.logWarning(
                        "command '%s' returned error: %n%s",
                        Arrays.toString(command),
                        new String(((StdErrEvent) event).data)
                    );
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
                    afterTermination.accept(exitCode == 0);
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
    }

    /**
     * Simple event forcing this thread to kill itself
     */
    private static class PoisonEvent implements Event
    {}

    public void shutdown()
    {
        started = false;
        handler.stop(true);
        if (thread != null)
        {
            thread.interrupt();
        }
        deque.addFirst(new PoisonEvent());
    }

    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (thread != null)
        {
            thread.join(timeoutRef);
        }
    }
}
