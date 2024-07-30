package com.linbit.extproc;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;
import com.linbit.utils.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.MDC;

/**
 * Runs an external command, logs and saves its output
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ExtCmd extends ChildProcessHandler
{
    private final Map<ExtCmdCondition, String> conditionsWithDescriptions;
    private final Set<ExtCmdEndedListener> extCmdEndedListenerSet;

    private OutputReceiver  outReceiver;
    private OutputReceiver  errReceiver;
    private ErrorReporter   errLog;
    private long            startTime;

    private String[] execCommand;
    private String execCommandStr;

    private boolean logExecution = true;
    private boolean saveWithoutSharedLocks = false;

    public ExtCmd(Timer<String, Action<String>> timer, ErrorReporter errLogRef)
    {
        super(timer);
        conditionsWithDescriptions = new HashMap<>();
        outReceiver = null;
        errReceiver = null;
        errLog = errLogRef;
        extCmdEndedListenerSet = new HashSet<>();
    }

    public ExtCmd setSaveWithoutSharedLocks(boolean saveWithoutSharedLocksRef)
    {
        saveWithoutSharedLocks = saveWithoutSharedLocksRef;
        return this;
    }

    public boolean isSaveWithoutSharedLocks()
    {
        return saveWithoutSharedLocks;
    }

    public void addCondition(ExtCmdCondition condition, String description)
    {
        conditionsWithDescriptions.put(condition, description);
    }

    public void addExtCmdEndedListener(ExtCmdEndedListener listenerRef)
    {
        extCmdEndedListenerSet.add(listenerRef);
    }

    public void asyncExec(String... command)
        throws IOException
    {
        exec(ProcessBuilder.Redirect.INHERIT, null, command);
    }

    public void pipeAsyncExec(ProcessBuilder.Redirect stdinRedirect, String... command)
        throws IOException
    {
        exec(ProcessBuilder.Redirect.PIPE, null, command);
    }

    public OutputData exec(String... command)
        throws IOException, ChildProcessTimeoutException
    {
        exec(ProcessBuilder.Redirect.INHERIT, null, command);
        return syncProcess();
    }

    public OutputData exec(File directory, String... command)
        throws IOException, ChildProcessTimeoutException
    {
        exec(ProcessBuilder.Redirect.INHERIT, directory, command);
        return syncProcess();
    }

    public OutputData pipeExec(ProcessBuilder.Redirect stdinRedirect, String... command)
        throws IOException, ChildProcessTimeoutException
    {
        exec(stdinRedirect, null, command);
        return syncProcess();
    }

    public OutputStream exec(ProcessBuilder.Redirect stdinRedirect, File directory, String... command)
        throws IOException
    {
        execCommand = command;
        execCommandStr = StringUtils.joinShellQuote(command);

        if (logExecution)
        {
            errLog.logDebug("Executing command: %s", execCommandStr);
        }

        ProcessBuilder pBuilder = new ProcessBuilder();
        pBuilder.directory(directory);
        pBuilder.command(command);
        pBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
        pBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pBuilder.redirectInput(stdinRedirect);
        Process child;
        synchronized (conditionsWithDescriptions)
        {
            checkForConditions();
            child = pBuilder.start();
            startTime = System.currentTimeMillis();
            setChild(child);
            outReceiver = new OutputReceiver(
                child.getInputStream(), errLog, logExecution, MDC.get(ErrorReporter.LOGID));
            errReceiver = new OutputReceiver(
                child.getErrorStream(), errLog, logExecution, MDC.get(ErrorReporter.LOGID));
        }
        new Thread(outReceiver).start();
        new Thread(errReceiver).start();

        return child.getOutputStream();
    }

    private void checkForConditions()
    {
        for (Entry<ExtCmdCondition, String> entry : conditionsWithDescriptions.entrySet())
        {
            if (!entry.getKey().isMet(this))
            {
                throw new ExtCmdConditionNotFullfilledException(entry.getValue() + ", command: " + this.execCommandStr);
            }
        }
    }

    public OutputData syncProcess() throws IOException, ChildProcessTimeoutException
    {
        OutputData outData;
        try
        {
            int exitCode = waitFor();
            outReceiver.finish();
            errReceiver.finish();
            outData = new OutputData(
                execCommand,
                outReceiver.getData(),
                errReceiver.getData(),
                exitCode
            );

            if (logExecution)
            {
                errLog.logTrace(
                    "External command finished in %dms: %s",
                    (System.currentTimeMillis() - startTime),
                    execCommandStr
                );
            }

            for (ExtCmdEndedListener listener : extCmdEndedListenerSet)
            {
                listener.extCmdEnded(this);
            }
        }
        catch (IOException | ChildProcessTimeoutException exc)
        {
            for (ExtCmdEndedListener listener : extCmdEndedListenerSet)
            {
                listener.extCmdEnded(this, exc);
            }
            throw exc;
        }
        return outData;
    }

    public void kill()
    {
        synchronized (conditionsWithDescriptions)
        {
            conditionsWithDescriptions.put(ignored -> false, "Process killed");
            try
            {
                waitForDestroy();
            }
            catch (ChildProcessTimeoutException termTimedOut)
            {
                waitForDestroyForcibly();
            }
            catch (ImplementationError implErr)
            {
                // process not started, childProcess was still null
                // this implErr can be ignored
            }
        }
    }

    public ExtCmd logExecution(boolean logRef)
    {
        logExecution = logRef;
        return this;
    }

    public static class OutputData
    {
        public final String[] executedCommand;
        public final byte[] stdoutData;
        public final byte[] stderrData;
        public final int exitCode;

        public OutputData(String[] executeCmd, byte[] out, byte[] err, int retCode)
        {
            executedCommand = executeCmd;
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

    public interface ExtCmdCondition
    {
        boolean isMet(ExtCmd extCmd);
    }

    public static class ExtCmdConditionNotFullfilledException extends LinStorRuntimeException
    {
        private static final long serialVersionUID = -3063355037719246587L;

        public ExtCmdConditionNotFullfilledException(String messageRef)
        {
            super(messageRef);
        }

        public ExtCmdConditionNotFullfilledException(
            String messageRef,
            String descriptionTextRef,
            String causeTextRef,
            String correctionTextRef,
            String detailsTextRef
        )
        {
            super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef);
        }

        public ExtCmdConditionNotFullfilledException(String messageRef, Throwable causeRef)
        {
            super(messageRef, causeRef);
        }
    }

    public interface ExtCmdEndedListener
    {
        void extCmdEnded(ExtCmd extCmd);

        void extCmdEnded(ExtCmd extCmd, Exception exc);
    }
}
