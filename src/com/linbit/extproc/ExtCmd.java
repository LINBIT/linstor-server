package com.linbit.extproc;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.timer.GenericTimer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Runs an external command, logs and saves its output
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ExtCmd extends ChildProcessHandler
{
    private OutputReceiver outReceiver;
    private OutputReceiver errReceiver;

    public ExtCmd(GenericTimer timer)
    {
        super(timer);
        outReceiver = null;
        errReceiver = null;
    }

    public void asyncExec(String... command)
        throws IOException
    {
        exec(ProcessBuilder.Redirect.INHERIT, command);
    }

    public void pipeAsyncExec(ProcessBuilder.Redirect stdinRedirect, String... command)
        throws IOException
    {
        exec(ProcessBuilder.Redirect.PIPE, command);
    }

    public OutputData exec(String... command)
        throws IOException, ChildProcessTimeoutException
    {
        exec(ProcessBuilder.Redirect.INHERIT, command);
        return syncProcess();
    }

    public OutputData pipeExec(ProcessBuilder.Redirect stdinRedirect, String... command)
        throws IOException, ChildProcessTimeoutException
    {
        exec(stdinRedirect, command);
        return syncProcess();
    }

    private void exec(ProcessBuilder.Redirect stdinRedirect, String... command)
        throws IOException
    {
        ProcessBuilder pBuilder = new ProcessBuilder();
        pBuilder.command(command);
        pBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
        pBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pBuilder.redirectInput(stdinRedirect);
        Process child = pBuilder.start();
        setChild(child);
        outReceiver = new OutputReceiver(child.getInputStream());
        errReceiver = new OutputReceiver(child.getErrorStream());
        new Thread(outReceiver).start();
        new Thread(errReceiver).start();
    }

    public OutputData syncProcess() throws IOException, ChildProcessTimeoutException
    {
        waitFor();
        outReceiver.finish();
        errReceiver.finish();
        OutputData outData = new OutputData(outReceiver.getData(), errReceiver.getData());
        return outData;
    }

    public static class OutputData
    {
        OutputData(byte[] out, byte[] err)
        {
            stdoutData = out;
            stderrData = err;
        }

        public InputStream getStdoutStream()
        {
            return new ByteArrayInputStream(stdoutData);
        }

        public InputStream getStderrStream()
        {
            return new ByteArrayInputStream(stderrData);
        }

        public byte[] stdoutData;
        public byte[] stderrData;
    }
}
