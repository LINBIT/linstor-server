package com.linbit.drbdmanage.logging;

import com.linbit.AutoIndent;
import com.linbit.drbdmanage.DrbdManage;
import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Standard error report generator
 *
 * Logs to SLF4J and writes detailed problem report files
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class StdErrorReporter implements ErrorReporter
{
    private static final String UNKNOWN_LABEL = "<UNKNOWN>";

    private static final String ERROR_FIELD_FORMAT = "%-32s    %s\n";

    private static final String LOG_DIRECTORY = "logs";

    private String dmModule;
    private final Logger mainLogger;
    private final Calendar cal;
    private final AtomicLong errorNr;

    public StdErrorReporter(String moduleName)
    {
        dmModule = moduleName;
        mainLogger = org.slf4j.LoggerFactory.getLogger(DrbdManage.PROGRAM + "/" + moduleName);
        errorNr = new AtomicLong();
        cal = Calendar.getInstance();
    }

    @Override
    public void reportError(Throwable errorInfo)
    {
        reportError(errorInfo, null, null, null);
    }

    @Override
    public void reportError(
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        PrintStream output = null;
        try
        {
            long reportNr = errorNr.getAndIncrement();
            String logMsg = formatLogMsg(reportNr, errorInfo);

            // Generate and report a null pointer exception if this
            // method is called with a null argument
            if (errorInfo == null)
            {
                errorInfo = new NullPointerException();
            }

            output = openReportFile(reportNr);

            // Error report header
            reportHeader(output, reportNr);

            // Report the error and any nested errors
            int loopCtr = 0;
            for (Throwable curErrorInfo = errorInfo; curErrorInfo != null; curErrorInfo = curErrorInfo.getCause())
            {
                if (loopCtr <= 0)
                {
                    output.println("Reported error:");
                }
                else
                {
                    output.println("Caused by:");
                }

                String category;
                if (curErrorInfo instanceof DrbdManageException)
                {
                    category = "DrbdManageException";

                    // Error description/cause/correction/details report
                    reportDrbdManageException(output, (DrbdManageException) curErrorInfo);
                }
                else
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

                // Determine exception class simple name
                String tClassName = UNKNOWN_LABEL;
                try
                {
                    Class tClass = curErrorInfo.getClass();
                    String simpleName = tClass.getSimpleName();
                    if (simpleName != null)
                    {
                        tClassName = simpleName;
                    }
                }
                catch (Exception ignored)
                {
                }

                // Determine exception class canonical name
                String tFullClassName = UNKNOWN_LABEL;
                try
                {
                    Class tClass = curErrorInfo.getClass();
                    String canName = tClass.getCanonicalName();
                    if (canName != null)
                    {
                        tFullClassName = canName;
                    }
                }
                catch (Exception ignored)
                {
                }

                // Determine the code location where the exception was generated
                String tGeneratedAt = UNKNOWN_LABEL;
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

                // Report information about the exception
                output.print(String.format(ERROR_FIELD_FORMAT, "Category:", category));
                output.print(String.format(ERROR_FIELD_FORMAT, "Class name:", tClassName));
                output.print(String.format(ERROR_FIELD_FORMAT, "Class canonical name:", tFullClassName));
                output.print(String.format(ERROR_FIELD_FORMAT, "Generated at:", tGeneratedAt));

                output.println();

                // Report the exception's message
                try
                {
                    String msg = curErrorInfo.getMessage();
                    if (msg != null)
                    {
                        output.print(String.format(ERROR_FIELD_FORMAT, "Error message:", msg));
                    }
                }
                catch (Exception ignored)
                {
                }

                output.println();

                if (loopCtr <= 0 && contextInfo != null)
                {
                    output.println("Error context:");
                    AutoIndent.printWithIndent(output, 4, contextInfo);
                    output.println();
                }

                // Report the call backtrace
                reportBacktrace(output, curErrorInfo);

                ++loopCtr;
            }

            output.println("\nEND OF ERROR REPORT.");

            mainLogger.error(logMsg);
        }
        finally
        {
            closeReportFile(output);
        }
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
        PrintStream output = null;
        try
        {
            long reportNr = errorNr.getAndIncrement();

            // If no description of the problem is available, log the technical details of the exception
            // as an error report instead.
            String message = errorInfo.getMessage();
            String descriptionMsg = errorInfo.getDescriptionText();
            if (descriptionMsg == null)
            {
                if (message == null)
                {
                    reportError(errorInfo, accCtx, client, contextInfo);
                }
                else
                {
                    descriptionMsg = message;
                }
            }

            if (descriptionMsg != null)
            {
                output = openReportFile(reportNr);

                // Error report header
                reportHeader(output, reportNr);

                // Error description/cause/correction/details report
                String causeMsg         = errorInfo.getCauseText();
                String correctionMsg    = errorInfo.getCorrectionText();
                String detailsMsg       = errorInfo.getDetailsText();

                output.println("Description:");
                AutoIndent.printWithIndent(output, 4, descriptionMsg);

                if (causeMsg != null)
                {
                    output.println("Cause:");
                    AutoIndent.printWithIndent(output, 4, causeMsg);
                }

                if (correctionMsg != null)
                {
                    output.println("Correction:");
                    AutoIndent.printWithIndent(output, 4, correctionMsg);
                }

                output.println();

                if (contextInfo != null)
                {
                    output.println("Error context:");
                    AutoIndent.printWithIndent(output, 4, contextInfo);
                    output.println();
                }

                if (accCtx != null)
                {
                    reportAccessContext(output, accCtx);
                }

                if (client != null)
                {
                    reportPeer(output, client);
                }

                // Report the error and any nested errors
                int loopCtr = 0;
                for (Throwable nestedErrorInfo = errorInfo.getCause();
                     nestedErrorInfo != null;
                     nestedErrorInfo = nestedErrorInfo.getCause())
                {
                    output.println("Caused by:\n");

                    if (nestedErrorInfo instanceof DrbdManageException)
                    {
                        reportDrbdManageException(output, (DrbdManageException) nestedErrorInfo);
                    }
                }

                String logMsg = formatLogMsg(reportNr, errorInfo);
                switch (logLevel)
                {
                    case ERROR:
                        mainLogger.error(logMsg);
                        break;
                    case WARN:
                        mainLogger.warn(logMsg);
                        break;
                    case INFO:
                        mainLogger.info(logMsg);
                        break;
                    case DEBUG:
                        mainLogger.debug(logMsg);
                        break;
                    case TRACE:
                        mainLogger.trace(logMsg);
                        break;
                    default:
                        mainLogger.error(logMsg);
                        reportError(
                            new IllegalArgumentException(
                                String.format(
                                    "Missing case label for enumeration value '%s'",
                                    logLevel.name()
                                )
                            )
                        );
                        break;
                }

                output.println("\nEND OF ERROR REPORT.\n");
            }
        }
        finally
        {
            closeReportFile(output);
        }
    }

    private String formatLogMsg(long reportNr, Throwable errorInfo)
    {
        // FIXME: check errorInfo == null
        String logMsg;
        String excMsg = errorInfo.getMessage();
        if (excMsg == null)
        {
            logMsg = String.format(
                "Problem of type '%s' logged to report number %d\n",
                errorInfo.getClass().getName(), reportNr
            );
        }
        else
        {
            logMsg = excMsg + String.format(
                " [Report number %d]\n",
                reportNr
            );
        }
        return logMsg;
    }

    private void reportAccessContext(PrintStream output, AccessContext accCtx)
    {
        output.println("Access context information\n");
        output.printf(ERROR_FIELD_FORMAT, "Identity:", accCtx.subjectDomain.name.displayValue);
        output.printf(ERROR_FIELD_FORMAT, "Role:", accCtx.subjectRole.name.displayValue);
        output.printf(ERROR_FIELD_FORMAT, "Domain:", accCtx.subjectDomain.name.displayValue);
        output.println();
    }

    private void reportPeer(PrintStream output, Peer client)
    {
        String peerAddress = null;
        int peerPort = 0;

        String peerId = client.getId();
        InetSocketAddress socketAddr = client.peerAddress();
        if (socketAddr != null)
        {
            InetAddress ipAddr = socketAddr.getAddress();
            peerPort = socketAddr.getPort();
            if (ipAddr != null)
            {
                peerAddress = ipAddr.getHostAddress();
            }
        }

        if (peerId != null)
        {
            output.println("Connected peer information\n");
            output.printf(ERROR_FIELD_FORMAT, "Peer ID:", peerId);
            if (peerAddress == null)
            {
                output.println("The peer's network address is unknown.");
            }
            else
            {
                output.printf(ERROR_FIELD_FORMAT, "Network address:", peerAddress);
            }
        }
    }

    private void reportHeader(PrintStream output, long reportNr)
    {
        output.print(String.format("ERROR REPORT %d\n\n", reportNr));
        output.printf(ERROR_FIELD_FORMAT, "Module:", dmModule);
        output.printf(ERROR_FIELD_FORMAT, "Version:", DrbdManage.VERSION);

        int year;
        int month;
        int day;
        int hour;
        int minute;
        int second;

        synchronized (cal)
        {
            cal.setTimeInMillis(System.currentTimeMillis());
            year    = cal.get(Calendar.YEAR);
            month   = cal.get(Calendar.MONTH);
            day     = cal.get(Calendar.DAY_OF_MONTH);

            hour    = cal.get(Calendar.HOUR);
            minute  = cal.get(Calendar.MINUTE);
            second  = cal.get(Calendar.SECOND);
        }

        output.printf(ERROR_FIELD_FORMAT, "Date:", String.format("%04d-%02d-%02d", year, month, day));
        output.printf(ERROR_FIELD_FORMAT, "Time:", String.format("%02d:%02d:%02d", hour, minute, second));

        output.println();
    }

    private boolean reportDrbdManageException(PrintStream output, DrbdManageException dmExc)
    {
        boolean detailsAvailable = false;

        String descriptionMsg   = dmExc.getDescriptionText();
        String causeMsg         = dmExc.getCauseText();
        String correctionMsg    = dmExc.getCorrectionText();
        String detailsMsg       = dmExc.getDetailsText();

        if (descriptionMsg != null)
        {
            detailsAvailable = true;
            output.println("Description:");
            AutoIndent.printWithIndent(output, 4, descriptionMsg);
        }

        if (causeMsg != null)
        {
            detailsAvailable = true;
            output.println("Cause:");
            AutoIndent.printWithIndent(output, 4, causeMsg);
        }

        if (correctionMsg != null)
        {
            detailsAvailable = true;
            output.println("Correction:");
            AutoIndent.printWithIndent(output, 4, correctionMsg);
        }

        if (detailsMsg != null)
        {
            detailsAvailable = true;
            output.println("Additional information:");
            AutoIndent.printWithIndent(output, 4, detailsMsg);
        }

        if (detailsAvailable)
        {
            output.println();
        }

        return detailsAvailable;
    }

    private void reportBacktrace(PrintStream output, Throwable errorInfo)
    {
        StackTraceElement[] trace = errorInfo.getStackTrace();
        if (trace == null)
        {
            output.println("No call backtrace is available.");
        }
        else
        {
            output.printf(
                "Call backtrace:\n\n" +
                "    %-40s %-6s %s\n",
                "Method", "Native", "Class:Line number"
            );
            for (StackTraceElement traceItem : trace)
            {
                boolean nativeCode  = traceItem.isNativeMethod();
                String methodName   = traceItem.getMethodName();
                int numericLineNr      = traceItem.getLineNumber();
                String fileName     = traceItem.getFileName();
                String className    = traceItem.getClassName();

                String lineNr;
                if (numericLineNr >= 0)
                {
                    lineNr = Integer.toString(numericLineNr);
                }
                else
                {
                    lineNr = "unknown";
                }

                if (methodName == null)
                {
                    methodName = "<Unknown method>";
                }
                if (className == null)
                {
                    output.printf("    %-40s %-6s\n", methodName, nativeCode ? "Y" : "N");
                    if (fileName != null)
                    {
                        output.printf("        - File: %-40s   Line nr.: %s\n", fileName, lineNr);
                    }
                }
                else
                {
                    output.printf(
                        "    %-40s %-6s %s:%s\n",
                        methodName, nativeCode ? "Y" : "N", className, lineNr
                    );
                }
            }
            output.println();
        }
    }

    private PrintStream openReportFile(long reportNr)
    {
        OutputStream reportStream = null;
        PrintStream reportPrinter = null;
        try
        {
            reportStream = new FileOutputStream(
                String.format(
                    "%s/ErrorReport-%06d.log",
                    LOG_DIRECTORY, reportNr
                )
            );
            reportPrinter = new PrintStream(reportStream);
        }
        catch (IOException ioExc)
        {
            System.err.printf("Unable to create error report file for error report %d:\n", reportNr);
            System.err.println(ioExc.getMessage());
            System.err.println("The error report will be written to the standard error stream instead.\n");

            if (reportPrinter != null)
            {
                try
                {
                    reportPrinter.close();
                }
                catch (Exception ignored)
                {
                }
                reportPrinter = null;
            }

            if (reportStream != null)
            {
                try
                {
                    reportStream.close();
                }
                catch (Exception ignored)
                {
                }
                reportStream = null;
            }
        }

        if (reportPrinter == null)
        {
            reportPrinter = System.err;
        }

        return reportPrinter;
    }

    private void closeReportFile(OutputStream output)
    {
        if (output != null && output != System.err)
        {
            try
            {
                output.close();
            }
            catch (IOException ignored)
            {
            }
        }
    }
}
