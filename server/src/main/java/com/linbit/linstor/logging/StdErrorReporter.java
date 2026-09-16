package com.linbit.linstor.logging;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.utils.TimeUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
    private final AtomicLong errorNr = new AtomicLong();
    private final Path baseLogDirectory;
    private final H2ErrorReporter h2ErrorReporter;

    public StdErrorReporter(
        String moduleName,
        Path logDirectory,
        boolean printStackTraces,
        String nodeName,
        @Nullable String logLevelRef,
        @Nullable String linstorLogLevelRef
    )
    {
        super(moduleName, printStackTraces, nodeName);
        this.baseLogDirectory = logDirectory;
        mainLogger = org.slf4j.LoggerFactory.getLogger(LinStor.PROGRAM + "/" + moduleName);

        // check if the log directory exists, generate if not
        File logDir = baseLogDirectory.toFile();
        if (!logDir.exists())
        {
            if (!logDir.mkdirs())
            {
                logError("Unable to create log directory: " + logDir);
            }
        }

        if (logLevelRef != null)
        {
            try
            {
                @Nullable String linstorLogLevel = linstorLogLevelRef;
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

        h2ErrorReporter = new H2ErrorReporter(this);

        logInfo("Log directory set to: '" + logDir + "'");

        System.setProperty("sentry.release", LinStor.VERSION_INFO_PROVIDER.getVersion());
        System.setProperty("sentry.servername", nodeName);
        System.setProperty("sentry.tags", "module:" + moduleName);
        System.setProperty("sentry.stacktrace.app.packages", "com.linbit");
        Sentry.init(options -> {
            options.setEnableExternalConfiguration(true);
            options.setDsn(""); // disable by default, can still be set via ENV or properties
            // https://docs.sentry.io/platforms/java/guides/spring-boot/configuration/#setting-the-dsn
        });
    }

    @Override
    public String getInstanceId()
    {
        return instanceId;
    }

    @Override
    public boolean hasAtLeastLogLevel(Level levelRef)
    {
        org.slf4j.Logger crtLogger = mainLogger;
        return switch (levelRef)
        {
            case DEBUG -> crtLogger.isDebugEnabled();
            case ERROR -> crtLogger.isErrorEnabled();
            case INFO -> crtLogger.isInfoEnabled();
            case TRACE -> crtLogger.isTraceEnabled();
            case WARN -> crtLogger.isWarnEnabled();
            default -> throw new ImplementationError("Unknown logging level: " + levelRef);
        };
    }

    @Override
    public @Nullable Level getCurrentLogLevel()
    {
        @Nullable Level level = null; // no logging, aka OFF
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
    public void setLogLevel(@Nullable Level level, @Nullable Level linstorLevel)
    {
        if (level != null || linstorLevel != null)
        {
            setLogLevelImpl(level, linstorLevel);
        }
    }

    /**
     * Sets the log-level to the given level if the logger uses Logback as a backend.
     *
     * @param level
     *     The level the root-logger, used for frameworks and libraries, will be set to.<br/>
     *     This does NOT influence linstor log messages.
     * @param linstorLevel
     *     The level the main-logger, used for linstor, will be set to.
     */
    private void setLogLevelImpl(@Nullable Level level, @Nullable Level linstorLevel)
    {
        // FIXME: Setting the trace mode only works with Logback as a backend,
        // but e.g. with SLF4J's SimpleLogger, this method has no effect
        org.slf4j.Logger crtLogger = org.slf4j.LoggerFactory.getLogger(
            Logger.ROOT_LOGGER_NAME
        );
        if (crtLogger instanceof ch.qos.logback.classic.Logger crtLogbackLogger)
        {
            if (level != null)
            {
                ch.qos.logback.classic.Level logBackLevel = ch.qos.logback.classic.Level.toLevel(level.toString());
                crtLogbackLogger.setLevel(logBackLevel);
            }
            if (linstorLevel != null)
            {
                if (mainLogger instanceof ch.qos.logback.classic.Logger logbackMainLogger)
                {
                    ch.qos.logback.classic.Level logBackLevel = ch.qos.logback.classic.Level
                        .toLevel(linstorLevel.toString());
                    logbackMainLogger.setLevel(logBackLevel);
                }
                else
                {
                    logError("MainLogger (linstor) is not a logback logger but the ROOT logger is!");
                }
            }
        }
    }

    @Override
    public @Nullable String reportError(Throwable errorInfo)
    {
        return reportError(Level.ERROR, errorInfo, null, null);
    }

    @Override
    public @Nullable String reportError(Level logLevel, Throwable errorInfo)
    {
        return reportError(logLevel, errorInfo, null, null);
    }

    @Override
    public @Nullable String reportError(
        Throwable errorInfo,
        @Nullable Peer client,
        @Nullable String contextInfo
    )
    {
        return reportImpl(Level.ERROR, errorInfo, client, contextInfo, true);
    }

    @Override
    public @Nullable String reportError(
        Level logLevel,
        Throwable errorInfo,
        @Nullable Peer client,
        @Nullable String contextInfo
    )
    {
        return reportImpl(logLevel, errorInfo, client, contextInfo, true);
    }

    @Override
    public @Nullable String reportProblem(
        Level logLevel,
        LinStorException errorInfo,
        @Nullable Peer client,
        @Nullable String contextInfo
    )
    {
        return reportImpl(logLevel, errorInfo, client, contextInfo, false);
    }

    private @Nullable String reportImpl(
        Level logLevel,
        Throwable errorInfo,
        @Nullable Peer client,
        @Nullable String contextInfo,
        boolean includeStackTrace
    )
    {
        @Nullable PrintStream output = null;
        long reportNr = errorNr.getAndIncrement();
        final String logName = getLogName(reportNr);
        final LocalDateTime errorTime = LocalDateTime.now(ZoneOffset.UTC);
        try
        {
            output = openReportFile(logName);

            // since we also want to include the report in the database, we should not directly write to our
            // output-PrintStream, but first render the report as a String and afterwards write the same string in the
            // output-PrintStream as well as give the byte[] of the String to the H2 error reporter
            ErrorReportRenderer errRepRenderer = new ErrorReportRenderer();

            renderReport(
                errRepRenderer,
                reportNr,
                client,
                errorInfo,
                errorTime,
                contextInfo,
                includeStackTrace
            );

            String renderedReport = errRepRenderer.getErrorReport();
            // write to PrintStream
            output.print(renderedReport);

            // write to H2
            h2ErrorReporter.writeErrorReportToDB(
                reportNr,
                client,
                errorInfo,
                instanceEpoch,
                errorTime,
                nodeName,
                dmModule,
                renderedReport.getBytes(StandardCharsets.UTF_8)
            );

            logReport(reportNr, errorInfo, logLevel);

            Sentry.captureException(errorInfo);
        }
        finally
        {
            closeReportFile(output);
        }
        return logName;
    }

    private void logReport(long reportNrRef, Throwable errorInfoRef, Level logLevelRef)
    {
        final String logMsg = formatLogMsg(reportNrRef, errorInfoRef);
        switch (logLevelRef)
        {
            case ERROR -> logError("%s", logMsg);
            case WARN -> logWarning("%s", logMsg);
            case INFO -> logInfo("%s", logMsg);
            case DEBUG -> logDebug("%s", logMsg);
            case TRACE -> logTrace("%s", logMsg);
            default ->
            {
                logError("%s", logMsg);
                reportError(
                    new IllegalArgumentException(
                        String.format(
                            "Missing case label for enumeration value '%s'",
                            logLevelRef.name()
                        )
                    )
                );
            }
        }
    }

    private String getLogName(long reportNr)
    {
        return String.format(
            "%s-%06d",
            instanceId,
            reportNr
        );
    }

    private Path getErrorLogPath(String logName)
    {
        return getLogDirectory().resolve(RPT_PREFIX + logName + RPT_SUFFIX);
    }

    private PrintStream openReportFile(String logName)
    {
        @Nullable PrintStream reportPrinter = null;
        try
        {
            Path filePath = getErrorLogPath(logName);
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

    private void closeReportFile(@Nullable OutputStream output)
    {
        if (output != null && !output.equals(System.err))
        {
            try
            {
                output.close();
            }
            catch (IOException ignored)
            {
                // ignored
            }
        }
    }

    @Override
    public ErrorReportResult listReports(
        boolean withText,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset,
        @Nullable final ErrorReportSortBy sortBy,
        @Nullable final Boolean sortAsc
    )
    {
        return h2ErrorReporter.listReports(withText, since, to, ids, limit, offset, sortBy, sortAsc);
    }

    @Override
    public ApiCallRc deleteErrorReports(
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            List<String> delIds = h2ErrorReporter.deleteErrorReports(since, to, exception, version, ids);

            for (String delId : delIds)
            {
                Path logFile = getErrorLogPath(delId);
                try
                {
                    if (Files.exists(logFile))
                    {
                        Files.delete(logFile);
                    }
                }
                catch (IOException e)
                {
                    apiCallRc.add(ApiCallRcImpl.entryBuilder(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "IO Error deleting text error report: " + logFile)
                        .setSkipErrorReport(true)
                        .build());
                }
            }

            if (!delIds.isEmpty())
            {
                apiCallRc.addEntry(String.format("Deleted %d error-report(s)", delIds.size()), ApiConsts.DELETED);
            }
        }
        catch (ApiRcException apiRcExc)
        {
            apiCallRc.addEntries(apiRcExc.getApiCallRc());
        }

        return apiCallRc;
    }

    private @Nullable BasicFileAttributes getAttributes(final Path file)
    {
        @Nullable BasicFileAttributes basicFileAttributes = null;
        try
        {
            basicFileAttributes = Files.readAttributes(file, BasicFileAttributes.class);
        }
        catch (IOException ignored)
        {
            // ignored
        }
        return basicFileAttributes;
    }

    @Override
    public void archiveLogDirectory(long ageDays)
    {
        if (ageDays <= 0)
        {
            logInfo("LogArchive: disabled (%s/%s is 0)", ApiConsts.NAMESPC_LOGGING, ApiConsts.KEY_LOG_ARCHIVE_AGE_DAYS);
        }
        else
        {
            // truncate the cutoff to the month boundary so that only complete months are ever archived.
            // the monthly tar files are overwritten (not appended to), so archiving a partially elapsed
            // month would replace an earlier archive of the same month, losing the already deleted reports
            archiveLogsOlderThan(
                LocalDate.now(ZoneId.systemDefault())
                    .minusDays(ageDays)
                    .withDayOfMonth(1)
                    .atStartOfDay(ZoneId.systemDefault())
                    .toInstant()
            );
        }
    }

    // package-private for testing
    void archiveLogsOlderThan(final Instant beforeDate)
    {
        try (Stream<Path> files = Files.list(getLogDirectory()))
        {
            logInfo(
                "LogArchive: Running log archive on directory: %s, archiving error-reports created before %s",
                getLogDirectory().toAbsolutePath().normalize(),
                beforeDate
            );
            final long startTime = System.currentTimeMillis();
            int archiveCount = 0;

            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM"); // grouping format

            Map<String, List<Path>> monthGroup = files
                .filter(file ->
                {
                    boolean ret;
                    @Nullable Path fileName = file.getFileName();
                    if (fileName == null)
                    {
                        ret = false;
                    }
                    else
                    {
                        ret = fileName.toString().startsWith(RPT_PREFIX) &&
                            fileName.toString().endsWith(RPT_SUFFIX);
                    }
                    return ret;
                })
                .filter(file ->
                {
                    // only archive regular files older than the given age. unexpected entries like
                    // directories are skipped entirely - neither archived nor deleted
                    @Nullable BasicFileAttributes attr = getAttributes(file);
                    boolean use = false;
                    if (attr != null)
                    {
                        if (attr.isRegularFile())
                        {
                            Instant createDate = Instant.ofEpochMilli(attr.creationTime().toMillis());
                            use = createDate.isBefore(beforeDate);
                        }
                        else
                        {
                            logWarning("LogArchive: Skipping unexpected non-file entry: %s", file);
                        }
                    }
                    return use;
                })
                .collect(Collectors.groupingBy(file ->
                {
                    @Nullable BasicFileAttributes attr = getAttributes(file);
                    return attr != null ? df.format(TimeUtils.millisToDate(attr.creationTime().toMillis())) : "unknown";
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
                    @Nullable Path fileName = logFile.getFileName();
                    // fileName should not be able to be null here, since the file would not have been added to
                    // monthGroup in that case, but sb complains anyways...
                    if (fileName != null)
                    {
                        fos.write(fileName.toString().getBytes(StandardCharsets.UTF_8));
                        fos.write("\n".getBytes(StandardCharsets.UTF_8));
                    }
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
                    int tarExitCode = createTar.waitFor();
                    if (tarExitCode == 0)
                    {
                        for (Path logFile : monthGroup.get(month))
                        {
                            try
                            {
                                Files.delete(logFile);
                            }
                            catch (IOException exc)
                            {
                                logWarning(
                                    "LogArchive: Unable to delete archived error-report %s: %s",
                                    logFile,
                                    exc
                                );
                            }
                        }
                    }
                    else
                    {
                        // do not delete anything that might not have made it into the archive
                        logWarning(
                            "LogArchive: tar returned exit code %d creating %s, keeping the error-reports " +
                                "of this month",
                            tarExitCode,
                            tarFile
                        );
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

    public void shutdown() throws DatabaseException
    {
        try
        {
            h2ErrorReporter.shutdown();
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
    }

}
