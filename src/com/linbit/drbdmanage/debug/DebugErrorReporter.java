package com.linbit.drbdmanage.debug;

import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.event.Level;

/**
 * Generates / formats error reports
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DebugErrorReporter implements ErrorReporter
{
    public static final String ID_FAILED = "<UNKNOWN>";

    private final AtomicLong errorNr;
    private final PrintStream defaultErrorOut;

    public DebugErrorReporter(PrintStream errorOutRef)
    {
        errorNr = new AtomicLong();
        defaultErrorOut = errorOutRef;
    }

    @Override
    public final void reportError(Throwable errorInfo)
    {
        reportError(errorInfo, System.err);
    }

    public final void reportError(Throwable errorInfo, PrintStream errorOut)
    {
        // Generate and report a null pointer exception if this
        // method is called with a null argument
        if (errorInfo == null)
        {
            errorInfo = new NullPointerException();
        }

        errorOut.println("\n\nERROR REPORT\n============\n");

        int loopCtr = 0;
        for (Throwable curErrorInfo = errorInfo; curErrorInfo != null; curErrorInfo = curErrorInfo.getCause())
        {
            if (loopCtr <= 0)
            {
                errorOut.println("Reported error:");
            }
            else
            {
                errorOut.println("Caused by:");
            }

            String category;
            if (curErrorInfo instanceof RuntimeException)
            {
                category = "RuntimeException";
            }
            else
            if (curErrorInfo instanceof Exception)
            {
                category = "Exception";
            }
            else
            if (curErrorInfo instanceof Error)
            {
                category = "Error";
            }
            else
            {
                category = "Throwable";
            }

            String tClassName = ID_FAILED;
            try
            {
                Class tClass = curErrorInfo.getClass();
                tClassName = notNullOrIdFailed(tClass.getSimpleName());
            }
            catch (Exception ignored)
            {
            }

            String tFullClassName = ID_FAILED;
            try
            {
                Class tClass = curErrorInfo.getClass();
                tFullClassName = notNullOrIdFailed(tClass.getCanonicalName());
            }
            catch (Exception ignored)
            {
            }

            String tMessage = "<NONE>";
            try
            {
                String msg = curErrorInfo.getMessage();
                if (msg != null)
                {
                    tMessage = msg;
                }
            }
            catch (Exception ignored)
            {
            }

            String tGeneratedAt = ID_FAILED;
            try
            {
                StackTraceElement[] traceItems = curErrorInfo.getStackTrace();
                if (traceItems != null)
                {
                    if (traceItems.length >= 1)
                    {
                        StackTraceElement topItem = traceItems[0];
                        if (topItem != null)
                        {
                            String methodName = topItem.getMethodName();
                            String fileName = topItem.getFileName();
                            String lineNumber = Integer.toString(topItem.getLineNumber());

                            StringBuilder result = new StringBuilder();
                            if (methodName != null)
                            {
                                result.append("Method '");
                                result.append(methodName);
                                result.append("'");
                            }
                            if (fileName != null)
                            {
                                if (result.length() > 0)
                                {
                                    result.append(", ");
                                }
                                result.append("Source file '");
                                result.append(fileName);
                                result.append("', Line #");
                                result.append(lineNumber);
                            }
                            if (result.length() > 0)
                            {
                                String resultStr = result.toString();
                                if (resultStr != null)
                                {
                                    tGeneratedAt = resultStr;
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ignored)
            {
            }

            printErrorField(errorOut, "Category", category);
            printErrorField(errorOut, "Class name", tClassName);
            printErrorField(errorOut, "Class path", tFullClassName);
            printErrorField(errorOut, "Generated at", tGeneratedAt);
            errorOut.println();
            printErrorField(errorOut, "Error message", tMessage);
            errorOut.println();

            errorOut.println("Call backtrace:\n---------------");
            curErrorInfo.printStackTrace(errorOut);
            errorOut.println();

            ++loopCtr;
        }

        errorOut.println("\nEND OF ERROR REPORT\n");
    }

    @Override
    public void logTrace(String message)
    {
        System.out.printf("%6s %s\n", "TRACE", message);
    }

    @Override
    public void logDebug(String message)
    {
        System.out.printf("%6s %s\n", "DEBUG", message);
    }

    @Override
    public void logInfo(String message)
    {
        System.out.printf("%6s %s\n", "INFO", message);
    }

    @Override
    public void logWarning(String message)
    {
        System.out.printf("%6s %s\n", "WARN", message);
    }

    @Override
    public void logError(String message)
    {
        System.out.printf("%6s %s\n", "ERROR", message);
    }

    public static final void printErrorField(
        PrintStream errorOut,
        String fieldName,
        String fieldContent
    )
    {
        errorOut.printf("  %-32s: %s\n", fieldName, fieldContent);
    }

    public static final String notNullOrIdFailed(String input)
    {
        return input != null ? input : ID_FAILED;
    }

    @Override
    public void reportError(
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        // TODO: include accCtx, client, contextInfo in the report
        reportError(errorInfo);
    }

    @Override
    public void reportProblem(
        Level logLevel,
        DrbdManageException errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        // TODO: include accCtx, client, contextInfo in the report
        reportError(errorInfo);
    }
}
