package com.linbit.linstor.clone;

import com.linbit.ImplementationError;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.OutputProxy;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.logging.ErrorReporter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class CloneDaemon implements Runnable
{
    private static final int DFLT_DEQUE_CAPACITY = 100;

    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final @Nullable String[] command;

    private final LinkedBlockingDeque<OutputProxy.Event> deque;
    private final @Nullable DaemonHandler handler;

    private boolean started = false;

    private final Consumer<Boolean> afterClone;

    public CloneDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        @Nullable String[] commandRef,
        Consumer<Boolean> afterTerminationRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;
        afterClone = afterTerminationRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        if (command == null)
        {
            handler = null;
        }
        else
        {
            handler = new DaemonHandler(deque, command);
        }


        thread = new Thread(threadGroupRef, this, threadName);
    }

    public void start()
    {
        started = true;
        thread.start();
        if (handler != null)
        {
            try
            {
                handler.startDelimited();
            }
            catch (IOException exc)
            {
                errorReporter.reportError(
                    new SystemServiceStartException(
                        "Unable to start daemon for Clone",
                        "I/O error attempting to start '" + Arrays.toString(command) + "'",
                        exc.getMessage(),
                        null,
                        null,
                        exc,
                        false
                    )
                );
            }
        }
    }

    @Override
    public void run()
    {
        boolean success = false;
        if (command != null)
        {
            String strCommand = Arrays.toString(command);
            errorReporter.logTrace("starting daemon: %s", strCommand);
            boolean first = true;
            StringBuilder sbErr = new StringBuilder();
            while (started)
            {
                OutputProxy.Event event;
                try
                {
                    event = deque.take();
                    if (event instanceof OutputProxy.StdOutEvent)
                    {
                        errorReporter.logTrace("stdOut: %s", new String(((OutputProxy.StdOutEvent) event).data));
                        // ignore for now...
                    }
                    else if (event instanceof OutputProxy.StdErrEvent)
                    {
                        String stdErr = new String(((OutputProxy.StdErrEvent) event).data);
                        sbErr.append(stdErr);
                        errorReporter.logWarning("stdErr: %s", stdErr);
                    }
                    else if (event instanceof OutputProxy.ExceptionEvent)
                    {
                        errorReporter.logTrace("ExceptionEvent in '%s':", Arrays.toString(command));
                        errorReporter.reportError(((OutputProxy.ExceptionEvent) event).exc);
                        // FIXME: Report the exception to the controller
                    }
                    else if (event instanceof CloneDaemon.PoisonEvent)
                    {
                        errorReporter.logTrace("PoisonEvent");
                        break;
                    }
                    else if (event instanceof OutputProxy.EOFEvent)
                    {
                        if (!first)
                        {
                            started = false;
                            int exitCode = handler.getExitCode();
                            errorReporter.logTrace("EOF. Exit code: " + exitCode);
                            success = exitCode == 0;
                            if (!success)
                            {
                                errorReporter.reportError(
                                    new LinStorException(
                                        "None 0 exit from: " + strCommand,
                                        "Clone command failed",
                                        sbErr.toString(),
                                        null,
                                        null));
                            }
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
                            "An exception of type " + exc.getClass().getSimpleName() +
                            " occurred while executing '" + Arrays.toString(command) + "'",
                            exc
                        )
                    );
                }
            }
        }
        else
        {
            success = true;
        }
        afterClone.accept(success);
    }

    /**
     * Simple event forcing this thread to kill itself
     */
    private static class PoisonEvent implements OutputProxy.Event
    {
    }

    public void shutdown()
    {
        started = false;
        if (handler != null)
        {
            handler.stop(false);
        }
        if (thread != null)
        {
            thread.interrupt();
        }
        deque.addFirst(new CloneDaemon.PoisonEvent());
    }

    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (thread != null)
        {
            thread.join(timeoutRef);
        }
    }
}
