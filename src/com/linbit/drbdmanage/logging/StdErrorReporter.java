package com.linbit.drbdmanage.logging;

import com.linbit.AutoIndent;
import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
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

    private static final String SECTION_SEPARATOR;
    private static final int SEPARATOR_WIDTH = 60;

    static
    {
        char[] separator = new char[SEPARATOR_WIDTH];
        Arrays.fill(separator, '=');
        SECTION_SEPARATOR = new String(separator);
    }

    private String dmModule;
    private final Logger mainLogger;
    private final Calendar cal;
    private final AtomicLong errorNr;
    private final String instanceId;

    public StdErrorReporter(String moduleName)
    {
        dmModule = moduleName;
        mainLogger = org.slf4j.LoggerFactory.getLogger(DrbdManage.PROGRAM + "/" + moduleName);

        errorNr = new AtomicLong();

        // Generate a unique instance ID based on the creation time of this instance
        instanceId = String.format("%07X", ((System.currentTimeMillis() / 1000) & 0xFFFFFFF));
        cal = Calendar.getInstance();

        // check if the log directory exists
        File logDir = new File(LOG_DIRECTORY);
        if (!logDir.exists())
        {
            logDir.mkdirs();
        }
    }

    @Override
    public String reportError(Throwable errorInfo)
    {
        return reportError(Level.ERROR, errorInfo, null, null, null);
    }


    @Override
    public String reportError(Level logLevel, Throwable errorInfo)
    {
        return reportError(logLevel, errorInfo, null, null, null);
    }

    @Override
    public String reportError(
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        return reportError(Level.ERROR, errorInfo, accCtx, client, contextInfo);
    }

    @Override
    public String reportError(
        Level logLevel,
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        PrintStream output = null;
        String logName = null;
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

            logName = getLogName(reportNr);
            output = openReportFile(logName);

            // Error report header
            reportHeader(output, reportNr);

            // Report the error and any nested errors
            int loopCtr = 0;
            for (Throwable curErrorInfo = errorInfo; curErrorInfo != null; curErrorInfo = curErrorInfo.getCause())
            {
                if (loopCtr <= 0)
                {
                    output.println("Reported error:\n===============\n");
                }
                else
                {
                    output.println("Caused by:\n==========\n");
                }


                reportExceptionDetails(output, curErrorInfo, loopCtr == 0 ? contextInfo : null);

                ++loopCtr;
            }

            output.println("\nEND OF ERROR REPORT.");

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
        }
        finally
        {
            closeReportFile(output);
        }
        return logName;
    }

    @Override
    public String reportProblem(
        Level logLevel,
        DrbdManageException errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        PrintStream output = null;
        String logName = null;
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
                logName = getLogName(reportNr);
                output = openReportFile(logName);

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
                    output.println("Caused by:\n==========\n");

                    if (nestedErrorInfo instanceof DrbdManageException)
                    {
                        boolean detailsAvailable = reportDrbdManageException(
                            output, (DrbdManageException) nestedErrorInfo
                        );
                        if (!detailsAvailable)
                        {
                            reportExceptionDetails(output, nestedErrorInfo, loopCtr == 0 ? contextInfo : null);
                        }
                    }
                    else
                    {
                        // FIXME: If a message is available, report the message,
                        //        else report as error
                    }

                    ++loopCtr;
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
        return logName;
    }

    private String formatLogMsg(long reportNr, Throwable errorInfo)
    {
        // FIXME: check errorInfo == null
        String logMsg;
        String excMsg = errorInfo.getMessage();
        if (excMsg == null)
        {
            logMsg = String.format(
                "Problem of type '%s' logged to report number %s-%06d\n",
                errorInfo.getClass().getName(), instanceId, reportNr
            );
        }
        else
        {
            logMsg = excMsg + String.format(
                " [Report number %s-%06d]\n",
                instanceId, reportNr
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
        output.print(String.format("ERROR REPORT %s-%06d\n\n", instanceId, reportNr));
        output.println(SECTION_SEPARATOR);
        output.println();
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
            month   = cal.get(Calendar.MONTH) + 1;
            day     = cal.get(Calendar.DAY_OF_MONTH);

            hour    = cal.get(Calendar.HOUR_OF_DAY);
            minute  = cal.get(Calendar.MINUTE);
            second  = cal.get(Calendar.SECOND);
        }

        output.printf(ERROR_FIELD_FORMAT, "Date:", String.format("%04d-%02d-%02d", year, month, day));
        output.printf(ERROR_FIELD_FORMAT, "Time:", String.format("%02d:%02d:%02d", hour, minute, second));

        output.println();
        output.println(SECTION_SEPARATOR);
        output.println();
    }

    private boolean reportDrbdManageException(PrintStream output, DrbdManageException dmExc)
    {
        boolean detailsAvailable = false;

        String descriptionMsg   = dmExc.getDescriptionText();
        String causeMsg         = dmExc.getCauseText();
        String correctionMsg    = dmExc.getCorrectionText();
        String detailsMsg       = dmExc.getDetailsText();

        if (descriptionMsg == null)
        {
            descriptionMsg = dmExc.getMessage();
        }

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

    private void reportExceptionDetails(PrintStream output, Throwable errorInfo, String contextInfo)
    {
        String category;
        if (errorInfo instanceof DrbdManageException)
        {
            category = "DrbdManageException";

            // Error description/cause/correction/details report
            reportDrbdManageException(output, (DrbdManageException) errorInfo);
        }
        else
        if (errorInfo instanceof RuntimeException)
        {
            category = "RuntimeException";
        }
        else
        if (errorInfo instanceof Exception)
        {
            category = "Exception";
        }
        else
        if (errorInfo instanceof Error)
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
            Class<? extends Throwable> tClass = errorInfo.getClass();
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
            Class<? extends Throwable> tClass = errorInfo.getClass();
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
            StackTraceElement[] traceItems = errorInfo.getStackTrace();
            if (traceItems != null)
            {
                if (traceItems.length >= 1)
                {
                    StackTraceElement topItem = traceItems[0];
                    if (topItem != null)
                    {
                        String methodName = topItem.getMethodName();
                        String fileName = topItem.getFileName();
                        int lineNumber = topItem.getLineNumber();

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
                            if (lineNumber >= 0)
                            {
                                result.append("', Line #");
                                result.append(Integer.toString(lineNumber));
                            }
                            else
                            {
                                result.append(", Unknown line number");
                            }
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
            String msg = errorInfo.getMessage();
            if (msg != null)
            {
                output.print(String.format(ERROR_FIELD_FORMAT, "Error message:", msg));
            }
        }
        catch (Exception ignored)
        {
        }

        output.println();

        if (contextInfo != null)
        {
            output.println("Error context:");
            AutoIndent.printWithIndent(output, 4, contextInfo);
            output.println();
        }

        // Report the call backtrace
        reportBacktrace(output, errorInfo);
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

    private String getLogName(long reportNr)
    {
        return String.format("%s-%06d",
            instanceId,
            reportNr
        );
    }

    private PrintStream openReportFile(String logName)
    {
        OutputStream reportStream = null;
        PrintStream reportPrinter = null;
        try
        {
            reportStream = new FileOutputStream(
                String.format(
                    "%s/ErrorReport-%s.log",
                    LOG_DIRECTORY,
                    logName
                )
            );
            reportPrinter = new PrintStream(reportStream);
        }
        catch (IOException ioExc)
        {
            System.err.printf("Unable to create error report file for error report %s:\n", logName);
            System.err.println(ioExc.getMessage());
            System.err.println("The error report will be written to the standard error stream instead.\n");
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

    @Override
    public void logTrace(String format, Object... args)
    {
        mainLogger.trace(String.format(format, args));
    }

    @Override
    public void logDebug(String format, Object... args)
    {
        mainLogger.debug(String.format(format, args));
    }

    @Override
    public void logInfo(String format, Object... args)
    {
        mainLogger.info(String.format(format, args));
    }

    @Override
    public void logWarning(String format, Object... args)
    {
        mainLogger.warn(String.format(format, args));
    }

    @Override
    public void logError(String format, Object... args)
    {
        mainLogger.error(String.format(format, args));
    }
}
