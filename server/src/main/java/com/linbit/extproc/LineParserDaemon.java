package com.linbit.extproc;

import com.linbit.ImplementationError;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.ShellUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class LineParserDaemon implements Runnable
{
    private static final int DFLT_DEQUE_CAPACITY = 100;

    private final ErrorReporter errorReporter;
    private final Thread thread;
    private final String[] command;

    private final LinkedBlockingDeque<OutputProxy.Event> deque;
    private final DaemonHandler handler;

    private boolean started = false;
    private String stdErr = "";
    private int exitCode = 0;

    private final Consumer<String> consumer;
    private final Object waiter = new Object();

    public LineParserDaemon(
        ErrorReporter errorReporterRef,
        ThreadGroup threadGroupRef,
        String threadName,
        String[] commandRef,
        Consumer<String> consumerRef
    )
    {
        errorReporter = errorReporterRef;
        command = commandRef;

        deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        handler = new DaemonHandler(deque, command);

        thread = new Thread(threadGroupRef, this, threadName);
        consumer = consumerRef;
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
                    "Unable to start LineParserDaemon for " + Arrays.toString(command),
                    "I/O error attempting to start '" + Arrays.toString(command) + "'",
                    exc.getMessage(),
                    null,
                    null,
                    exc,
                    false
                )
            );
            throw new LinStorRuntimeException("Start of LineParserDaemon failed", exc);
        }
    }

    @Override
    public void run()
    {
        String strCommand = ShellUtils.joinShellQuote(command);
        errorReporter.logTrace("Executing command: %s", strCommand);
        boolean first = true;
        StringBuilder sbErr = new StringBuilder();
        long startTime = System.currentTimeMillis();
        while (started)
        {
            OutputProxy.Event event;
            try
            {
                event = deque.take();
                if (event instanceof OutputProxy.StdOutEvent)
                {
                    String line = new String(((OutputProxy.StdOutEvent) event).data);
                    consumer.accept(line);
                    errorReporter.logTrace(line);
                }
                else if (event instanceof OutputProxy.StdErrEvent)
                {
                    String stdErrLine = new String(((OutputProxy.StdErrEvent) event).data);
                    sbErr.append(stdErrLine);
                    errorReporter.logWarning("stdErr: %s", stdErrLine);
                }
                else if (event instanceof OutputProxy.ExceptionEvent)
                {
                    errorReporter.logError("ExceptionEvent in '%s':", strCommand);
                    errorReporter.reportError(((OutputProxy.ExceptionEvent) event).exc);
                }
                else if (event instanceof LineParserDaemon.PoisonEvent)
                {
                    errorReporter.logTrace("PoisonEvent");
                    break;
                }
                else if (event instanceof OutputProxy.EOFEvent)
                {
                    if (!first)
                    {
                        started = false;
                        exitCode = handler.getExitCode();
                        if (exitCode != 0)
                        {
                            errorReporter.reportError(
                                new LinStorException(
                                    String.format("Exit code %d from: %s", exitCode, strCommand),
                                    "lsblk command failed",
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
        synchronized (waiter)
        {
            stdErr = sbErr.toString();
            errorReporter.logTrace(
                "External command finished in %dms: %s",
                (System.currentTimeMillis() - startTime),
                strCommand
            );
            waiter.notify();
        }
    }

    public int waitDone()
    {
        synchronized (waiter)
        {
            try
            {
                waiter.wait(ExtCmd.dfltWaitTimeout);
            }
            catch (InterruptedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
        return exitCode;
    }

    public String getStdErr()
    {
        return stdErr;
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
        handler.stop(false);
        synchronized (waiter)
        {
            waiter.notifyAll();
        }
        thread.interrupt();
        deque.addFirst(new LineParserDaemon.PoisonEvent());
    }

    public void awaitShutdown(long timeoutRef) throws InterruptedException
    {
        thread.join(timeoutRef);
    }
}
