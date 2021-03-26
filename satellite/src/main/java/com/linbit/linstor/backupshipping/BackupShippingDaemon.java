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
import com.linbit.linstor.storage.StorageException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AmazonS3Exception;

public class BackupShippingDaemon implements Runnable
{
    private static final int DFLT_DEQUE_CAPACITY = 100;
    private final ErrorReporter errorReporter;
    private final Thread cmdThread;
    private final Thread s3Thread;
    private final String[] command;
    private final BackupToS3 backupHandler;
    private final String backupName;
    private final String bucketName;
    private final long volSize;

    private final LinkedBlockingDeque<Event> deque;
    private final DaemonHandler handler;
    private final Object syncObj = new Object();

    private boolean running = false;
    private Process cmdProcess;
    private boolean afterTerminationSent = false;
    private boolean doneFirst = true;
    private boolean restore;
    private String uploadId = null;

    private final Consumer<Boolean> afterTermination;

    public BackupShippingDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        String backupNameRef,
        String bucketNameRef,
        BackupToS3 backupHandlerRef,
        boolean restoreRef,
        long size,
        Consumer<Boolean> afterTerminationRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;
        bucketName = bucketNameRef;
        afterTermination = afterTerminationRef;
        backupName = backupNameRef;
        backupHandler = backupHandlerRef;
        volSize = size;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);

        cmdThread = new Thread(threadGroupRef, this, threadName);
        restore = restoreRef;

        if (restore)
        {
            s3Thread = new Thread(threadGroupRef, this::runRestoring, "backup_" + threadName);
        }
        else
        {
            handler.setStdOutListener(false);
            s3Thread = new Thread(threadGroupRef, this::runShipping, "backup_" + threadName);
        }
    }

    public String start()
    {
        running = true;
        cmdThread.start();
        String uploadIdRef = null;
        if (!restore)
        {
            uploadIdRef = backupHandler.initMultipart(backupName);
        }
        try
        {
            cmdProcess = handler.start();
            s3Thread.start();
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
        uploadId = uploadIdRef;
        return uploadIdRef;
    }

    private void runShipping()
    {
        errorReporter.logTrace("starting sending for backup %s", backupName);
        boolean success = false;
        try
        {
            backupHandler.putObjectMultipart(backupName, cmdProcess.getInputStream(), volSize, uploadId);
            success = true;
        }
        catch (SdkClientException | IOException | StorageException exc)
        {
            if (!success && running)
            {
                // closing the stream might throw exceptions, which do not need to be reported if we already
                // successfully finished previously
                errorReporter.reportError(exc);
            }
        }
        threadFinished(success, true);
    }

    private void runRestoring()
    {
        errorReporter.logTrace("starting restore for backup %s", backupName);
        boolean success = false;
        try (
            InputStream is = backupHandler.getObject(backupName, bucketName);
            OutputStream os = cmdProcess.getOutputStream();
        )
        {
            byte[] readBuf = new byte[1 << 20];
            int readLen = 0;
            while ((readLen = is.read(readBuf)) != -1)
            {
                os.write(readBuf, 0, readLen);
            }
            os.flush();
            Thread.sleep(500);
            success = true;
        }
        catch (SdkClientException | IOException | InterruptedException exc)
        {
            if (!success && running)
            {
                // closing the streams might throw exceptions, which do not need to be reported if we already
                // successfully finished previously
                errorReporter.reportError(exc);
            }
        }
        threadFinished(success, true);
    }

    @Override
    public void run()
    {
        errorReporter.logTrace("starting daemon: %s", Arrays.toString(command));
        boolean success = false;
        boolean first = true;
        while (running)
        {
            Event event;
            try
            {
                event = deque.take();
                if (event instanceof StdOutEvent)
                {
                    errorReporter.logTrace("stdOut: %s", new String(((StdOutEvent) event).data));
                    // should not exist when sending due to handler.setStdOutListener(false)
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
                    success = exitCode == 0;
                    if (restore && !first || !restore)
                    {
                        running = false;
                    }
                    else
                    {
                        first = false;
                    }
                }
            }
            catch (InterruptedException exc)
            {
                if (running)
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
        threadFinished(success, true);
    }

    private void threadFinished(boolean success, boolean shutdownAllowed)
    {
        synchronized (syncObj)
        {
            if (shutdownAllowed)
            {
                errorReporter.logTrace("thread finished");
            }
            if (doneFirst)
            {
                doneFirst = false;
                if (!success)
                {
                    afterTerminationSent = true;
                    afterTermination.accept(success);
                    try
                    {
                        if (uploadId != null)
                        {
                            backupHandler.abortMultipart(backupName, uploadId);
                        }
                    }
                    catch (SdkClientException exc)
                    {
                        if (exc.getClass() == AmazonS3Exception.class)
                        {
                            AmazonS3Exception s3Exc = (AmazonS3Exception) exc;
                            if (s3Exc.getStatusCode() != 404)
                            {
                                errorReporter.reportError(exc);
                            }
                        }
                        else
                        {
                            errorReporter.reportError(exc);
                        }
                    }
                    if (shutdownAllowed)
                    {
                        shutdown();
                    }
                }
            }
            else if (!afterTerminationSent)
            {
                afterTerminationSent = true;
                afterTermination.accept(success);
                if (!success)
                {
                    try
                    {
                        if (uploadId != null)
                        {
                            backupHandler.abortMultipart(backupName, uploadId);
                        }
                    }
                    catch (SdkClientException exc)
                    {
                        if (exc.getClass() == AmazonS3Exception.class)
                        {
                            AmazonS3Exception s3Exc = (AmazonS3Exception) exc;
                            if (s3Exc.getStatusCode() != 404)
                            {
                                errorReporter.reportError(exc);
                            }
                        }
                        else
                        {
                            errorReporter.reportError(exc);
                        }
                    }
                }
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
        threadFinished(false, false);
        running = false;
        handler.stop(false);
        if (cmdThread != null)
        {
            cmdThread.interrupt();
        }
        if (s3Thread != null)
        {
            s3Thread.interrupt();
        }
        deque.addFirst(new PoisonEvent());
    }

    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        if (cmdThread != null)
        {
            cmdThread.join(timeoutRef);
        }
        if (s3Thread != null)
        {
            s3Thread.join(timeoutRef);
        }
    }
}
