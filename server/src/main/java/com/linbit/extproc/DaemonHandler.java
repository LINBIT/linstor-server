package com.linbit.extproc;

import com.linbit.ImplementationError;
import com.linbit.extproc.OutputProxy.Event;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

public class DaemonHandler
{
    public static final int PROCESS_STOPPED = Integer.MAX_VALUE;
    private static final byte DELIMITER = '\n';

    private final ProcessBuilder processBuilder;
    private Process process;

    private Thread outThread;
    private Thread errThread;
    private OutputProxy errProxy;
    private OutputProxy outProxy;

    private final BlockingDeque<Event> deque;
    private boolean stdOut;

    public DaemonHandler(final BlockingDeque<Event> dequeRef, final String... command)
    {
        deque = dequeRef;
        processBuilder = new ProcessBuilder(command);
        processBuilder.redirectError(Redirect.PIPE);
        stdOut = true;
    }

    public void setStdOutListener(boolean stdOutRef)
    {
        stdOut = stdOutRef;
    }

    public Process start() throws IOException
    {
        stop(true);

        process = processBuilder.start();
        errProxy = new OutputProxy(process.getErrorStream(), deque, DELIMITER, false);
        errThread = new Thread(errProxy);
        if (stdOut)
        {
            outProxy = new OutputProxy(process.getInputStream(), deque, DELIMITER, true);
            outThread = new Thread(outProxy);
            outThread.start();
        }
        errThread.start();
        return process;
    }

    public void stop(boolean force)
    {
        if (process != null)
        {
            if (outProxy != null)
            {
                outProxy.expectShutdown();
            }
            errProxy.expectShutdown();
            if (force)
            {
                process.destroyForcibly();
            }
            else
            {
                process.destroy();
            }
            if (outThread != null)
            {
                outThread.interrupt();
            }
            errThread.interrupt();
            process = null;
        }
    }

    public int getExitCode()
    {
        int exitValue;
        Process proc = process;
        if (proc != null)
        {
            if (proc.isAlive())
            {
                try
                {
                    proc.waitFor(500, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ignored)
                {
                }
                if (proc.isAlive())
                {
                    throw new ImplementationError("Process is still running");
                }
            }
            exitValue = proc.exitValue();
        }
        else
        {
            exitValue = PROCESS_STOPPED;
        }
        return exitValue;
    }
}
