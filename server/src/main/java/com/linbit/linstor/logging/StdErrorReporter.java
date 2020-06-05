package com.linbit.linstor.logging;

import com.linbit.AutoIndent;
import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import javax.inject.Provider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.sentry.Sentry;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Standard error report generator
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
    private Provider<AccessContext> peerCtxProvider;

    public StdErrorReporter(
        String moduleName,
        Path logDirectory,
        boolean printStackTraces,
        String nodeName,
        String logLevelRef,
        String linstorLogLevelRef,
        Provider<AccessContext> peerCtxProviderRef
    )
    {
        super(moduleName, printStackTraces, nodeName);
        this.baseLogDirectory = logDirectory;
        peerCtxProvider = peerCtxProviderRef;
        mainLogger = org.slf4j.LoggerFactory.getLogger(LinStor.PROGRAM + "/" + moduleName);

        errorNr = new AtomicLong();

        // Generate a unique instance ID based on the creation time of this instance

        // check if the log directory exists, generate if not
        File logDir = baseLogDirectory.toFile();
        if (!logDir.exists())
        {
            logDir.mkdirs();
        }

        if (logLevelRef != null)
        {
            try
            {

                String linstorLogLevel = linstorLogLevelRef;
                if (linstorLogLevel == null)
                {
                    linstorLogLevel = logLevelRef;
                }
                setLogLevelImpl(
                    Level.valueOf(logLevelRef.toUpperCase()),
                    Level.valueOf(linstorLogLevel.toUpperCase())
                );
            }
            catch (IllegalArgumentException exc)
            {
                logError("Invalid log level '%s'", logLevelRef);
            }
        }

        logInfo("Log directory set to: '" + logDir + "'");

        System.setProperty("sentry.release", LinStor.VERSION_INFO_PROVIDER.getVersion());
        System.setProperty("sentry.servername", nodeName);
        System.setProperty("sentry.tags", "module:" + moduleName);
        System.setProperty("sentry.stacktrace.app.packages", "com.linbit");
        Sentry.init();
    }

    @Override
    public String getInstanceId()
    {
        return instanceId;
    }

    @Override
    public boolean hasAtLeastLogLevel(Level levelRef)
    {
        boolean hasRequiredLevel = true;
        org.slf4j.Logger crtLogger = mainLogger;
        switch (levelRef)
        {
            case DEBUG:
                hasRequiredLevel = crtLogger.isDebugEnabled();
                break;
            case ERROR:
                hasRequiredLevel = crtLogger.isErrorEnabled();
                break;
            case INFO:
                hasRequiredLevel = crtLogger.isInfoEnabled();
                break;
            case TRACE:
                hasRequiredLevel = crtLogger.isTraceEnabled();
                break;
            case WARN:
                hasRequiredLevel = crtLogger.isWarnEnabled();
                break;
            default:
                throw new ImplementationError("Unknown logging level: " + levelRef);
        }
        return hasRequiredLevel;
    }

    @Override
    public Level getCurrentLogLevel()
    {
        Level level = null; // no logging, aka OFF
        org.slf4j.Logger crtLogger = mainLogger;
        if (crtLogger.isTraceEnabled())
        {
            level = Level.TRACE;
        }
        else
        if (crtLogger.isDebugEnabled())
        {
            level = Level.DEBUG;
        }
        else
        if (crtLogger.isInfoEnabled())
        {
            level = Level.INFO;
        }
        else
        if (crtLogger.isWarnEnabled())
        {
            level = Level.WARN;
        }
        else
        if (crtLogger.isErrorEnabled())
        {
            level = Level.ERROR;
        }
        return level;
    }

    @Override
    public boolean setLogLevel(AccessContext accCtx, Level level, Level linstorLevel)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        boolean success = true;
        if (level != null && linstorLevel != null)
        {
            success = setLogLevelImpl(level, linstorLevel);
        }
        else
        if (level != null)
        {
            success = setLogLevelImpl(level, null);
        }
        else
        if (linstorLevel != null)
        {
            success = setLogLevelImpl(null, linstorLevel);
        }
        return success;
    }

    private boolean setLogLevelImpl(Level level, Level linstorLevel)
    {
        // FIXME: Setting the trace mode only works with Logback as a backend,
        // but e.g. with SLF4J's SimpleLogger, this method has no effect
        org.slf4j.Logger crtLogger = org.slf4j.LoggerFactory.getLogger(
            Logger.ROOT_LOGGER_NAME
        );
        boolean success = false;
        if (crtLogger instanceof ch.qos.logback.classic.Logger)
        {
            ch.qos.logback.classic.Logger crtLogbackLogger = (ch.qos.logback.classic.Logger) crtLogger;

            if (level != null)
            {
                ch.qos.logback.classic.Level logBackLevel = ch.qos.logback.classic.Level.toLevel(level.toString());
                crtLogbackLogger.setLevel(logBackLevel);
                success = true;
            }
            if (mainLogger instanceof ch.qos.logback.classic.Logger && linstorLevel != null)
            {
                ch.qos.logback.classic.Level logBackLevel = ch.qos.logback.classic.Level
                    .toLevel(linstorLevel.toString());
                ((ch.qos.logback.classic.Logger) mainLogger).setLevel(logBackLevel);
            }
            else
            {
                logError("MainLogger (linstor) is not a logback logger but the ROOT logger is!");
            }
        }
        return success;
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

                Throwable[] suppressedExceptions = curErrorInfo.getSuppressed();
                for (int supIdx = 0; supIdx < suppressedExceptions.length; ++supIdx)
                {
                    output.printf(
                        "Suppressed exception %d of %d:\n===============\n",
                        supIdx + 1,
                        suppressedExceptions.length
                    );

                    reportExceptionDetails(output, suppressedExceptions[supIdx], loopCtr == 0 ? contextInfo : null);
                }

                ++loopCtr;
            }

            output.println("\nEND OF ERROR REPORT.");

            switch (logLevel)
            {
                case ERROR:
                    logError(logMsg);
                    break;
                case WARN:
                    logWarning(logMsg);
                    break;
                case INFO:
                    logInfo(logMsg);
                    break;
                case DEBUG:
                    logDebug(logMsg);
                    break;
                case TRACE:
                    logTrace(logMsg);
                    break;
                default:
                    logError(logMsg);
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

            Sentry.capture(errorInfo);
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
                for (
                    Throwable nestedErrorInfo = errorInfo.getCause();
                    nestedErrorInfo != null;
                    nestedErrorInfo = nestedErrorInfo.getCause()
                )
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
                        logError(logMsg);
                        break;
                    case WARN:
                        logWarning(logMsg);
                        break;
                    case INFO:
                        logInfo(logMsg);
                        break;
                    case DEBUG:
                        logDebug(logMsg);
                        break;
                    case TRACE:
                        logTrace(logMsg);
                        break;
                    default:
                        logError(logMsg);
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

                Sentry.capture(errorInfo);
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
        return String.format(
            "%s-%06d",
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
        final Set<String> ids
    )
    {
        TreeSet<ErrorReport> errors = new TreeSet<>();
        final List<String> fileIds = ids.stream().map(s -> "ErrorReport-" + s).collect(Collectors.toList());

        Function<FileTime, Boolean> sinceFct;
        if (since.isPresent())
        {
            long sinceTimestamp = since.get().getTime();
            sinceFct = (fileTime) -> sinceTimestamp < fileTime.toMillis();
        }
        else
        {
            sinceFct = (fileTime) -> true;
        }

        Function<FileTime, Boolean> toFct;
        if (to.isPresent())
        {
            long toTimestamp = to.get().getTime();
            toFct = (fileTime) -> toTimestamp > fileTime.toMillis();
        }
        else
        {
            toFct = (fileTime) -> true;
        }

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

                            if (sinceFct.apply(attr.creationTime()) && toFct.apply(attr.creationTime()))
                            {
                                StringBuilder sb = new StringBuilder();
                                if (withText)
                                {
                                    try (BufferedReader br = Files.newBufferedReader(file))
                                    {
                                        String line = br.readLine();
                                        while (line != null)
                                        {
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
                        catch (IOException /* | ParseException */ ignored)
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

    private BasicFileAttributes getAttributes(final Path file)
    {
        BasicFileAttributes basicFileAttributes = null;
        try
        {
            basicFileAttributes = Files.readAttributes(file, BasicFileAttributes.class);
        }
        catch (IOException ignored)
        {
        }
        return basicFileAttributes;
    }

    @Override
    public void archiveLogDirectory()
    {
        try (Stream<Path> files = Files.list(getLogDirectory()))
        {
            logInfo("LogArchive: Running log archive on directory: " + getLogDirectory().toAbsolutePath().normalize());
            final long startTime = System.currentTimeMillis();
            int archiveCount = 0;
            // create a Date instance that is starting 2 months before.
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MONTH, -1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            Date beforeDate = cal.getTime();

            DateFormat df = new SimpleDateFormat("yyyy-MM"); // grouping format

            Map<String, List<Path>> monthGroup = files
                .filter(file -> file.getFileName().toString().startsWith("ErrorReport"))
                .filter(file ->
                {
                    // only archive files older 2 months (at month starting)
                    BasicFileAttributes attr = getAttributes(file);
                    boolean use = false;
                    if (attr != null)
                    {
                        Date createDate = new Date(attr.creationTime().toMillis());
                        use = createDate.before(beforeDate);
                    }
                    return use;
                })
                .collect(Collectors.groupingBy(file ->
                {
                    BasicFileAttributes attr = getAttributes(file);
                    return attr != null ? df.format(attr.creationTime().toMillis()) : "unknown";
                }));

            for (String month : monthGroup.keySet())
            {
                final Path tarFile = getLogDirectory()
                    .toAbsolutePath()
                    .normalize()
                    .resolve("log-archive-" + month + ".tar.gz");

                File tempLogFiles = File.createTempFile("logarchive-", "-" + month);
                FileOutputStream fos = new FileOutputStream(tempLogFiles);
                for (Path logFile : monthGroup.get(month))
                {
                    archiveCount++;
                    fos.write(logFile.getFileName().toString().getBytes());
                    fos.write("\n".getBytes());
                }
                fos.close();

                Process createTar = new ProcessBuilder(
                    "tar",
                    "-czf", tarFile.toString(),
                    "-C", getLogDirectory().toString(),
                    "-T", tempLogFiles.toString()
                ).start();
                try
                {
                    createTar.waitFor();

                    for (Path logFile : monthGroup.get(month))
                    {
                        Files.delete(logFile);
                    }
                }
                catch (InterruptedException exc)
                {
                    throw new LinStorRuntimeException("Unable to tar.gz log archive: " + tarFile.toString(), exc);
                }

                tempLogFiles.deleteOnExit();
            }
            if (archiveCount > 0)
            {
                logInfo("LogArchive: Archived %d logs in %dms", archiveCount, System.currentTimeMillis() - startTime);
            }
            else
            {
                logInfo("LogArchive: No logs to archive.");
            }
        }
        catch (IOException exc)
        {
            throw new LinStorRuntimeException("Unable to list log directory", exc);
        }
    }

    @Override
    public Path getLogDirectory()
    {
        return baseLogDirectory;
    }

    @Override
    public void logTrace(String format, Object... args)
    {
        mainLogger.trace(appendUserNameAndFormat(format, args));
    }

    @Override
    public void logDebug(String format, Object... args)
    {
        mainLogger.debug(appendUserNameAndFormat(format, args));
    }

    @Override
    public void logInfo(String format, Object... args)
    {
        mainLogger.info(appendUserNameAndFormat(format, args));
    }

    @Override
    public void logWarning(String format, Object... args)
    {
        mainLogger.warn(appendUserNameAndFormat(format, args));
    }

    @Override
    public void logError(String format, Object... args)
    {
        mainLogger.error(appendUserNameAndFormat(format, args));
    }

    private String appendUserNameAndFormat(String formatRef, Object... args)
    {
        String user;
        AccessContext peerCtx = peerCtxProvider.get();
        if (peerCtx == null)
        {
            user = "SYSTEM";
        }
        else
        {
            user = peerCtx.subjectId.name.displayValue;
        }
        // extending the format with "%s" would be easy, but extending the var args would include
        // a system array copy.
        return String.format(user + " - " + formatRef, args);
    }
}
