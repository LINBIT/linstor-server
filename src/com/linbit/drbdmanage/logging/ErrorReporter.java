package com.linbit.drbdmanage.logging;

import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import org.slf4j.event.Level;

/**
 * Generates / formats error reports
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface ErrorReporter
{
    // TODO: javadoc

    void logTrace(String format, Object... args);
    void logDebug(String format, Object... args);
    void logInfo(String format, Object... args);
    void logWarning(String format, Object... args);
    void logError(String format, Object... args);

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
     * @param errorInfo
     */
    void reportError(Throwable errorInfo);

    /**
     * Reports any kind of error, especially ones that are not expected during normal operation
     *
     * @param errorInfo
     * @param accCtx
     * @param client
     * @param contextInfo
     */
    void reportError(
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        // Information about the context in which the problem occured, e.g., the API call being performed
        String contextInfo
    );

    /**
     * Reports less severe problems, such as the ones expected during normal operation of drbdmanage
     *
     * E.g., a StorageException caused by running out of space, an exhausted numbers pool, a user-specified
     * value being out of range, etc.
     *
     * @param errorInfo
     * @param accCtx
     * @param client
     * @param contextInfo
     */
    void reportProblem(
        Level logLevel,
        DrbdManageException errorInfo,
        AccessContext accCtx,
        Peer client,
        // Information about the context in which the problem occured, e.g., the API call being performed
        String contextInfo
    );
}
