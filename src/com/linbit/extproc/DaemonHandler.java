package com.linbit.extproc;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.concurrent.BlockingDeque;

import com.linbit.extproc.OutputProxy.Event;

public class DaemonHandler
{
    private final static byte DELIMITER = '\n';

    private final ProcessBuilder processBuilder;
    private Process process;

    private Thread outThread;
    private Thread errThread;
    private OutputProxy errProxy;
    private OutputProxy outProxy;

    private final BlockingDeque<Event> deque;

    public DaemonHandler(final BlockingDeque<Event> deque, final String... command)
    {
        this.deque = deque;
        processBuilder = new ProcessBuilder(command);
        processBuilder.redirectError(Redirect.PIPE);
    }

    public void start() throws IOException
    {
        if (process != null)
        {
            stop(true);
        }

        process = processBuilder.start();
        outProxy = new OutputProxy(process.getInputStream(), deque, DELIMITER, true);
        errProxy = new OutputProxy(process.getErrorStream(), deque, DELIMITER, false);
        outThread = new Thread(outProxy);
        errThread = new Thread(errProxy);
        outThread.start();
        errThread.start();
    }

    public void stop(boolean force)
    {
        outProxy.expectShutdown();
        errProxy.expectShutdown();
        if (force)
        {
            // FIXME: if migrated to java 8, call process.destroyForcibly()
            process.destroy();
        }
        else
        {
            process.destroy();
        }
        outThread.interrupt();
        errThread.interrupt();
        process = null;
    }
}
