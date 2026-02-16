package com.linbit.utils;

import com.linbit.extproc.ChildProcessHandler;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

public class CommandExec
{
    /**
     * Executes the given command and stores the stdOut and stdErr in the given files. If the always created error-file
     * is empty after the command finishes, it is deleted again.
     *
     *
     * @return Whether the error file exists or not after this method
     *
     */
    public static boolean executeCmd(
        final String[] commandRef,
        final java.io.File outFileRef,
        final java.io.File errFileRef,
        final long timepstampRef
    )
        throws IOException, InterruptedException
    {
        // we would need to rework ExtCmd to support redirecting Out and Err into given files instead of collecting them
        // in the OutputData.
        ProcessBuilder pb = new ProcessBuilder(commandRef);
        pb.redirectOutput(outFileRef);
        pb.redirectError(errFileRef);

        Process proc = pb.start();
        final long timeout = ChildProcessHandler.getDefaultWaitTimeout();
        boolean exited = proc.waitFor(timeout, TimeUnit.MILLISECONDS);
        if (!exited)
        {
            proc.destroyForcibly();
            Files.write(
                errFileRef.toPath(),
                ("\n\nCommand did not terminate within " + timeout + "ms. Command was: " +
                    ShellUtils.joinShellQuote(commandRef)).getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE
            );
        }

        outFileRef.setLastModified(timepstampRef);
        boolean errFileExists;
        if (errFileRef.length() == 0)
        {
            Files.delete(errFileRef.toPath());
            errFileExists = false;
        }
        else
        {
            errFileRef.setLastModified(timepstampRef);
            errFileExists = true;
        }
        return errFileExists;
    }

    /**
     * Converts the exception's stacktrace to a String
     */
    public static String exceptionToString(Exception exc)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exc.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }
}
