package com.linbit.extproc;

import com.linbit.ImplementationError;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.linstor.annotation.Nullable;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

public class DaemonHandler
{
    public static final int PROCESS_STOPPED = Integer.MAX_VALUE;
    private static final byte DELIMITER = '\n';

    private final ProcessBuilder processBuilder;
    private @Nullable Process process;

    private @Nullable Thread outThread;
    private @Nullable Thread errThread;
    private @Nullable OutputProxy errProxy;
    private @Nullable OutputProxy outProxy;

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

    public Process startDelimited() throws IOException
    {
        return startDelimited(DELIMITER);
    }

    public Process startDelimited(byte delimiterRef) throws IOException
    {
        stop(true);

        process = processBuilder.start();
        errProxy = new OutputProxyDelimited(process.getErrorStream(), deque, delimiterRef, false);
        errThread = new Thread(errProxy);
        if (stdOut)
        {
            outProxy = new OutputProxyDelimited(process.getInputStream(), deque, delimiterRef, true);
            outThread = new Thread(outProxy);
            outThread.start();
        }
        errThread.start();
        return process;
    }

    /**
     * Starts the process and produces events splitting after {@link OutputProxy#DFLT_BUFFER_SIZE} bytes
     *
     * @return
     *
     * @throws IOException
     */
    public Process startUndelimited() throws IOException
    {
        return startUndelimited(OutputProxy.DFLT_BUFFER_SIZE);
    }

    public Process startUndelimited(int bufferSize) throws IOException
    {
        stop(true);

        process = processBuilder.start();
        errProxy = new OutputProxy(process.getErrorStream(), deque, false, bufferSize);
        errThread = new Thread(errProxy);
        if (stdOut)
        {
            outProxy = new OutputProxy(process.getInputStream(), deque, true, bufferSize);
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
