package com.linbit.linstor.logging;

import com.linbit.AutoIndent;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Standard error report generator
 *
 * Logs to SLF4J and writes detailed problem report files
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public final class StdErrorReporter extends BaseErrorReporter implements ErrorReporter
{
    public static final String RPT_PREFIX = "ErrorReport-";
    public static final String RPT_SUFFIX = ".log";

    private final Logger mainLogger;
    private final AtomicLong errorNr;
    private final Path baseLogDirectory;

    public StdErrorReporter(String moduleName, Path logDirectory, boolean printStackTraces, String nodeName)
    {
        super(moduleName, printStackTraces, nodeName);
        this.baseLogDirectory = logDirectory;
        mainLogger = org.slf4j.LoggerFactory.getLogger(LinStor.PROGRAM + "/" + moduleName);

        errorNr = new AtomicLong();

        // Generate a unique instance ID based on the creation time of this instance

        // check if the log directory exists, generate if not
        File logDir = baseLogDirectory.toFile();
        if (!logDir.exists())
        {
            logDir.mkdirs();
        }

        logInfo("Log directory set to: '" + logDir + "'");
    }

    @Override
    public String getInstanceId()
    {
        return instanceId;
    }

    @Override
    public boolean isTraceEnabled()
    {
        // FIXME: Trace mode is currently only implemented with a Logback backend
        boolean traceMode = true;
        org.slf4j.Logger crtLogger = org.slf4j.LoggerFactory.getLogger(
            ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME
        );
        if (crtLogger instanceof ch.qos.logback.classic.Logger)
        {
            ch.qos.logback.classic.Logger crtLogbackLogger = (ch.qos.logback.classic.Logger) crtLogger;
            traceMode = crtLogbackLogger.getLevel() == ch.qos.logback.classic.Level.TRACE;
        }
        return traceMode;
    }

    @Override
    public void setTraceEnabled(AccessContext accCtx, boolean traceMode)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        // FIXME: Setting the trace mode only works with Logback as a backend,
        // but e.g. with SLF4J's SimpleLogger, this method has no effect
        org.slf4j.Logger crtLogger = org.slf4j.LoggerFactory.getLogger(
            ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME
        );
        if (crtLogger instanceof ch.qos.logback.classic.Logger)
        {
            ch.qos.logback.classic.Logger crtLogbackLogger = (ch.qos.logback.classic.Logger) crtLogger;
            crtLogbackLogger.setLevel(
                traceMode ? ch.qos.logback.classic.Level.TRACE : ch.qos.logback.classic.Level.DEBUG
            );
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
        return reportErrorImpl(logLevel, errorInfo, accCtx, client, contextInfo);
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
            final Throwable checkedErrorInfo = errorInfo == null ? new NullPointerException() : errorInfo;

            logName = getLogName(reportNr);
            output = openReportFile(logName);

            // Error report header
            reportHeader(output, reportNr, client);

            // Report the error and any nested errors
            int loopCtr = 0;
            for (
                Throwable curErrorInfo = checkedErrorInfo;
                curErrorInfo != null;
                curErrorInfo = curErrorInfo.getCause()
            )
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
        return reportProblemImpl(logLevel, errorInfo, accCtx, client, contextInfo);
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
                reportHeader(output, reportNr, client);

                reportLinStorException(output, errorInfo);

                if (contextInfo != null)
                {
                    output.println("Error context:");
                    AutoIndent.printWithIndent(output, AutoIndent.DEFAULT_INDENTATION, contextInfo);
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
                if (errorInfo.getCause() != null && printStackTraces)
                {
                    errorInfo.getCause().printStackTrace();
                }
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
                        descriptionMsg = nestedErrorInfo.getMessage();
                        if (descriptionMsg != null)
                        {
                            output.println("Description:");
                            AutoIndent.printWithIndent(output, AutoIndent.DEFAULT_INDENTATION, descriptionMsg);
                            output.println();
                        }
                        reportExceptionDetails(output, nestedErrorInfo, loopCtr == 0 ? contextInfo : null);
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
        PrintStream reportPrinter = null;
        try
        {
            Path filePath = getLogDirectory().resolve(RPT_PREFIX + logName + RPT_SUFFIX);
            OutputStream reportStream = new FileOutputStream(
                filePath.toFile()
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

    public static Set<ErrorReport> listReports(
        final String nodeName,
        final Path logDirectory,
        boolean withText,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids)
    {
        TreeSet<ErrorReport> errors = new TreeSet<>();
        final Date now = new Date(System.currentTimeMillis());
        final List<String> fileIds = ids.stream().map(s -> "ErrorReport-" + s).collect(Collectors.toList());

        if (fileIds.isEmpty())
        {
            // if id filter is disabled, add a match all filter.
            fileIds.add("ErrorReport-");
        }

        try (Stream<Path> files = Files.list(logDirectory))
        {
            files.filter(file -> file.getFileName().toString().startsWith("ErrorReport"))
                .filter(
                    file ->
                    {
                        boolean ret = false;
                        for (String fileId : fileIds)
                        {
                            if (file.getFileName().toString().startsWith(fileId))
                            {
                                ret = true;
                                break;
                            }
                        }
                        return ret;
                    }
                )
                .forEach(
                    file ->
                    {
                        String fileName = file.getFileName().toString();

                        try
                        {
                            BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);

                            if (since.orElse(new Date(0)).getTime() < attr.creationTime().toMillis() &&
                                attr.creationTime().toMillis() < to.orElse(now).getTime())
                            {
                                StringBuilder sb = new StringBuilder();
                                if (withText)
                                {
                                    try (BufferedReader br = Files.newBufferedReader(file))
                                    {
                                        String line = br.readLine();
                                        while (line != null)
                                        {
    //                                    if (date == null && line.startsWith(errorTimeLabel))
    //                                    {
    //                                        String dateStr = line.substring(errorTimeLabel.length()).trim();
    //                                        date = TIMESTAMP_FORMAT.parse(dateStr);
    //                                    }

                                            sb.append(line).append('\n');

                                            line = br.readLine();
                                        }
                                    }
                                }
                                errors.add(new ErrorReport(
                                    nodeName,
                                    fileName,
                                    new Date(attr.creationTime().toMillis()),
                                    sb.toString())
                                );
                            }
                        }
                        catch (IOException /*| ParseException*/ ignored)
                        {
                        }
                    }
                );
        }
        catch (IOException ignored)
        {
        }

        return errors;
    }

    @Override
    public Path getLogDirectory()
    {
        return baseLogDirectory;
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
