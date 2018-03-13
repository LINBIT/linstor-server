package com.linbit.extproc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;
import com.linbit.utils.StringUtils;

/**
 * Runs an external command, logs and saves its output
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ExtCmd extends ChildProcessHandler
{
    private static final Pattern SPACE_PATTERN = Pattern.compile(" ");

    private OutputReceiver  outReceiver;
    private OutputReceiver  errReceiver;
    private ErrorReporter   errLog;

    public ExtCmd(Timer<String, Action<String>> timer, ErrorReporter errLogRef)
    {
        super(timer);
        outReceiver = null;
        errReceiver = null;
        errLog = errLogRef;
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

    public OutputStream exec(ProcessBuilder.Redirect stdinRedirect, String... command)
        throws IOException
    {
        logCommandExecution(command);

        ProcessBuilder pBuilder = new ProcessBuilder();
        pBuilder.command(command);
        pBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
        pBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pBuilder.redirectInput(stdinRedirect);
        Process child = pBuilder.start();
        setChild(child);
        outReceiver = new OutputReceiver(child.getInputStream(), errLog);
        errReceiver = new OutputReceiver(child.getErrorStream(), errLog);
        new Thread(outReceiver).start();
        new Thread(errReceiver).start();

        return child.getOutputStream();
    }

    private void logCommandExecution(String... command)
    {
        List<String> commandElements = new ArrayList<>();

        for (String commandElement : command)
        {
            commandElements.add(SPACE_PATTERN.matcher(commandElement).replaceAll("\\\\ "));
        }

        errLog.logDebug("Executing command: " + StringUtils.join(commandElements, " "));
    }

    public OutputData syncProcess() throws IOException, ChildProcessTimeoutException
    {
        int exitCode = waitFor();
        outReceiver.finish();
        errReceiver.finish();
        OutputData outData = new OutputData(outReceiver.getData(), errReceiver.getData(), exitCode);
        return outData;
    }

    public static class OutputData
    {
        public byte[] stdoutData;
        public byte[] stderrData;
        public int exitCode;

        protected OutputData(byte[] out, byte[] err, int retCode)
        {
            stdoutData = out;
            stderrData = err;
            exitCode = retCode;
        }

        public InputStream getStdoutStream()
        {
            return new ByteArrayInputStream(stdoutData);
        }

        public InputStream getStderrStream()
        {
            return new ByteArrayInputStream(stderrData);
        }
    }
}
