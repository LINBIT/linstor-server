package com.linbit.linstor.logging;

import com.linbit.AutoIndent;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
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
public final class StdErrorReporter extends BaseErrorReporter implements ErrorReporter
{
    public static final String LOG_DIRECTORY = "logs";
    public static final String RPT_PREFIX = "ErrorReport-";
    public static final String RPT_SUFFIX = ".log";

    private static final String RPT_ID_TRACE_DISABLED = "TRACE_LEVEL_REPORTING_DISABLED";

    private volatile boolean traceEnabled = false;

    private final Logger mainLogger;
    private final AtomicLong errorNr;
    private final String baseLogDirectory;

    public StdErrorReporter(String moduleName, String logDirectory)
    {
        super(moduleName);
        this.baseLogDirectory = logDirectory;
        mainLogger = org.slf4j.LoggerFactory.getLogger(LinStor.PROGRAM + "/" + moduleName);

        errorNr = new AtomicLong();

        // Generate a unique instance ID based on the creation time of this instance

        // check if the log directory exists
        File logDir = new File(baseLogDirectory + LOG_DIRECTORY);
        if (!logDir.exists())
        {
            logDir.mkdirs();
        }
    }

    @Override
    public String getInstanceId()
    {
        return instanceId;
    }

    @Override
    public boolean isTraceEnabled()
    {
        return traceEnabled;
    }

    @Override
    public void setTraceEnabled(AccessContext accCtx, boolean flag)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        traceEnabled = flag;
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
        return reportErrorImpl(Level.ERROR, errorInfo, accCtx, client, contextInfo);
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
        String reportId = RPT_ID_TRACE_DISABLED;
        if (traceEnabled || logLevel != Level.TRACE )
        {
            reportId = reportErrorImpl(logLevel, errorInfo, accCtx, client, contextInfo);
        }
        return reportId;
    }

    private String reportErrorImpl(
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
        LinStorException errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        String reportId = RPT_ID_TRACE_DISABLED;
        if (traceEnabled || logLevel != Level.TRACE)
        {
            reportId = reportProblemImpl(logLevel, errorInfo, accCtx, client, contextInfo);
        }
        return reportId;
    }

    private String reportProblemImpl(
        Level logLevel,
        LinStorException errorInfo,
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

                    if (nestedErrorInfo instanceof LinStorException)
                    {
                        boolean detailsAvailable = reportLinStorException(
                            output, (LinStorException) nestedErrorInfo
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
            reportStream = new FileOutputStream(LOG_DIRECTORY + "/" + RPT_PREFIX + logName + RPT_SUFFIX);
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
        if (traceEnabled)
        {
            mainLogger.trace(String.format(format, args));
        }
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
