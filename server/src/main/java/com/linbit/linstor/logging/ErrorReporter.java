package com.linbit.linstor.logging;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.netcom.Peer;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.event.Level;

/**
 * Generates / formats error reports
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ErrorReporter
{
    String LOGID = "logid";

    int DFLT_LOG_ARCHIVE_AGE_DAYS = 60;

    // TODO: javadoc
    /**
     * Indicates if at least the given LogLevel is enabled.
     * ERROR < WARN < INFO < DEBUG < TRACE
     */
    boolean hasAtLeastLogLevel(Level level);

    /**
     * Returns the current log level;
     *
     */
    @Nullable Level getCurrentLogLevel();

    /**
     * Sets the log level, if the backing logging frameworks supports that.
     *
     * @param level
     *     The log-level that the logger for frameworks and libraries should use.<br/>
     *     Does NOT influence linstor log messages.
     * @param linstorLevel
     *     The log-level that the logger for linstor should use.
     *
     */
    void setLogLevel(@Nullable Level level, @Nullable Level linstorLevel);

    void logTrace(String format, Object... args);

    void logDebug(String format, Object... args);

    void logInfo(String format, Object... args);

    void logWarning(String format, Object... args);

    void logError(String format, Object... args);

    /**
     * Returns the instance ID of the error reporter instance
     *
     * @return Instance ID of this error reporter instance
     */
    String getInstanceId();

    /**
     * Reports any kind of error, especially ones that are not expected during normal operation
     *
     * E.g., internal errors that may require debugging, detected implementation errors,
     * inability to load parts of the program (missing class files), etc.
     *
     * Implementations of the methods specified in this interface are not supposed to throw any
     * exceptions, not even RuntimeExceptions, because if the ErrorReporter is not working,
     * there is no way to report such exceptions anyway.
     * As a last resort, logging may fall back to printing to the standard error output.
     * Obviously, errors - such as an OutOfMemoryError - may be generated while running the
     * error reporter. Such errors will mostly likely lead to the termination of the program,
     * or may be logged to the standard error output if appropriate, or may be ignored completely
     * by whatever component of the program is trying to use the respective ErrorReporter
     * implementation.
     * This method calls {@link ErrorReporter#reportError(Level, Throwable)} with
     * {@link Level#ERROR} as default logLevel.
     *
     *
     * @return the logName of the generated report; may be null if no report was created
     */
    @Nullable
    String reportError(Throwable errorInfo);

    /**
     * Reports any kind of error, especially ones that are not expected during normal operation
     *
     * E.g., internal errors that may require debugging, detected implementation errors,
     * inability to load parts of the program (missing class files), etc.
     *
     * Implementations of the methods specified in this interface are not supposed to throw any
     * exceptions, not even RuntimeExceptions, because if the ErrorReporter is not working,
     * there is no way to report such exceptions anyway.
     * As a last resort, logging may fall back to printing to the standard error output.
     * Obviously, errors - such as an OutOfMemoryError - may be generated while running the
     * error reporter. Such errors will mostly likely lead to the termination of the program,
     * or may be logged to the standard error output if appropriate, or may be ignored completely
     * by whatever component of the program is trying to use the respective ErrorReporter
     * implementation.
     *
     *
     * @return the logName of the generated report; may be null if no report was created
     */
    @Nullable
    String reportError(Level logLevel, Throwable errorInfo);

    /**
     * Reports any kind of error, especially ones that are not expected during normal operation.
     * Calls {@link ErrorReporter#reportError(Level, Throwable, Peer, String)}
     * with {@link Level#ERROR} as default logLevel.
     *
     *
     * @return the logName of the generated report; may be null if no report was created
     */
    @Nullable String reportError(
        Throwable errorInfo,
        @Nullable Peer client,
        // Information about the context in which the problem occurred, e.g., the API call being performed
        String contextInfo
    );

    /**
     * Reports any kind of error, especially ones that are not expected during normal operation, with
     * the specified logLevel.
     *
     *
     * @return the logName of the generated report; may be null if no report was created
     */
    @Nullable String reportError(
        Level logLevel,
        Throwable errorInfo,
        @Nullable Peer client,
        // Information about the context in which the problem occurred, e.g., the API call being performed
        @Nullable String contextInfo
    );

    /**
     * Reports less severe problems, such as the ones expected during normal operation of linstor
     *
     * E.g., a StorageException caused by running out of space, an exhausted numbers pool, a user-specified
     * value being out of range, etc.
     *
     *
     * @return the logName of the generated report; may be null if no report was created
     */
    @Nullable String reportProblem(
        Level logLevel,
        LinStorException errorInfo,
        @Nullable Peer client,
        // Information about the context in which the problem occurred, e.g., the API call being performed
        String contextInfo
    );

    Path getLogDirectory();

    default ErrorReportResult listReports(
        boolean withText,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset
    )
    {
        return listReports(withText, since, to, ids, limit, offset, null, null);
    }

    default ErrorReportResult listReports(
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
        return new ErrorReportResult(0, Collections.emptyList());
    }

    default ApiCallRc deleteErrorReports(
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        return new ApiCallRcImpl();
    }

    /**
     * Archives and compresses error-report log files older than the given number of days.
     * The cutoff is truncated to the month boundary, so files are only archived once their whole
     * creation-month is older than the given age; each month is compressed into its own archive.
     *
     * @param ageDays
     *     Minimum age in days after which error-report log files are archived;
     *     values &lt;= 0 disable archiving.
     */
    default void archiveLogDirectory(long ageDays)
    {
    }

    static String getNewLogId()
    {
        String zeros = "000000";
        Random rnd = new Random();
        String s = Integer.toString(rnd.nextInt(0X1000000), 16);
        return zeros.substring(s.length()) + s;
    }
}
