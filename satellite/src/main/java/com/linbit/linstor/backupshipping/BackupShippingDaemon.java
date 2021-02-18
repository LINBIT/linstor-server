package com.linbit.linstor.backupshipping;

import com.linbit.ImplementationError;
import com.linbit.SystemServiceStartException;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.OutputProxy.EOFEvent;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;
import com.linbit.linstor.api.BackupToS3;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import com.amazonaws.SdkClientException;

public class BackupShippingDaemon implements Runnable
{
    private static final int DFLT_DEQUE_CAPACITY = 100;
    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final Thread shippingThread;
    private final String[] command;
    private final BackupToS3 backupHandler;
    private final String backupName;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;
    private final Object syncObj = new Object();

    private boolean started = false;
    private Process sendingProcess;
    private boolean finished = false;

    private final Consumer<Boolean> afterTermination;

    public BackupShippingDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        String backupNameRef,
        BackupToS3 backupHandlerRef,
        Consumer<Boolean> afterTerminationRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;
        afterTermination = afterTerminationRef;
        backupName = backupNameRef;
        backupHandler = backupHandlerRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);
        handler.setStdOutListener(false);

        thread = new Thread(threadGroupRef, this, threadName);
        shippingThread = new Thread(threadGroupRef, this::runShipping, "backup_" + threadName);
    }

    public void start()
    {
        started = true;
        thread.start();
        try
        {
            sendingProcess = handler.start();
            shippingThread.start();
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
    }

    private void runShipping()
    {
        try
        {
            backupHandler.putObject(backupName, sendingProcess.getInputStream(), null);
            synchronized (syncObj)
            {
                if (!finished)
                {
                    finished = true;
                    afterTermination.accept(true);
                }
            }
        }
        catch (SdkClientException exc)
        {
            errorReporter.reportError(exc);
            synchronized (syncObj)
            {
                if (!finished)
                {
                    finished = true;
                    afterTermination.accept(false);
                }
            }
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
                else if (event instanceof StdErrEvent)
                {
                    String stdErr = new String(((StdErrEvent) event).data);
                    errorReporter.logWarning("stdErr: %s", stdErr);
                }
                else if (event instanceof ExceptionEvent)
                {
                    errorReporter.logTrace("ExceptionEvent in '%s':", Arrays.toString(command));
                    errorReporter.reportError(((ExceptionEvent) event).exc);
                    // FIXME: Report the exception to the controller
                }
                else if (event instanceof PoisonEvent)
                {
                    errorReporter.logTrace("PoisonEvent");
                    break;
                }
                else if (event instanceof EOFEvent)
                {
                    int exitCode = handler.getExitCode();
                    errorReporter.logTrace("EOF. Exit code: " + exitCode);
                    if (exitCode != 0)
                    {
                        synchronized (syncObj)
                        {
                            if (!finished)
                            {
                                finished = true;
                                afterTermination.accept(false);
                            }
                        }
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
        if (thread != null)
        {
            thread.interrupt();
        }
        if (shippingThread != null)
        {
            shippingThread.interrupt();
        }
        deque.addFirst(new PoisonEvent());
    }

    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (thread != null)
        {
            thread.join(timeoutRef);
        }
        if (shippingThread != null)
        {
            shippingThread.join(timeoutRef);
        }
    }
}
