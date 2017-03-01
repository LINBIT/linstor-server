package com.linbit.drbdmanage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates / formats error reports
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ErrorReporter
{
    public static final String ID_FAILED = "<UNKNOWN>";

    private AtomicLong errorNr;

    public ErrorReporter()
    {
        errorNr = new AtomicLong();
    }

    public final void reportError(Throwable errorInfo)
    {
        // Generate and report a null pointer exception if this
        // method is called with a null argument
        if (errorInfo == null)
        {
            errorInfo = new NullPointerException();
        }

        System.err.println("\n\nERROR REPORT\n============\n");

        int loopCtr = 0;
        for (Throwable curErrorInfo = errorInfo; curErrorInfo != null; curErrorInfo = curErrorInfo.getCause())
        {
            if (loopCtr <= 0)
            {
                System.err.println("Reported error:");
            }
            else
            {
                System.err.println("Caused by:");
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

            printErrorField("Category", category);
            printErrorField("Class name", tClassName);
            printErrorField("Class path", tFullClassName);
            printErrorField("Generated at", tGeneratedAt);
            System.err.println();
            printErrorField("Error message", tMessage);
            System.err.println();

            System.err.println("Call backtrace:\n---------------");
            curErrorInfo.printStackTrace(System.err);
            System.err.println();

            ++loopCtr;
        }

        System.err.println("\nEND OF ERROR REPORT\n");
    }

    public static final void printErrorField(String fieldName, String fieldContent)
    {
        System.err.printf("  %-32s: %s\n", fieldName, fieldContent);
    }

    public static final String notNullOrIdFailed(String input)
    {
        return input != null ? input : ID_FAILED;
    }
}
