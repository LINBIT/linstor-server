package com.linbit.drbdmanage.testutils;

import org.slf4j.event.Level;

import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

public class DefaultErrorStreamErrorReporter implements ErrorReporter
{
    @Override
    public String getInstanceId()
    {
        // Hex instance ID of drbdmanageNG's error reporter
        // Not significant for the test, just needs to return something to implement the interface
        return "CAFEAFFE";
    }

    @Override
    public void logTrace(String format, Object... args)
    {
        System.err.printf(format, args);
    }

    @Override
    public void logDebug(String format, Object... args)
    {
        System.err.printf(format, args);
    }

    @Override
    public void logInfo(String format, Object... args)
    {
        System.err.printf(format, args);
    }

    @Override
    public void logWarning(String format, Object... args)
    {
        System.err.printf(format, args);
    }

    @Override
    public void logError(String format, Object... args)
    {
        System.err.printf(format, args);
    }

    @Override
    public String reportError(Throwable errorInfo)
    {
        errorInfo.printStackTrace(System.err);
        return null; // no error report, no logName
    }

    @Override
    public String reportError(Level logLevel, Throwable errorInfo)
    {
        errorInfo.printStackTrace(System.err);
        return null; // no error report, no logName
    }

    @Override
    public String reportError(Throwable errorInfo, AccessContext accCtx, Peer client, String contextInfo)
    {
        System.err.println("AccCtx: Identity      : " + accCtx.subjectId.name.value + "\n" +
                           "        SecurityDomain: " + accCtx.subjectDomain.name.value + "\n" +
                           "        Role          :" + accCtx.subjectRole.name.value
        );
        System.err.println("Peer id: " + client.getId());
        System.err.println(contextInfo);
        errorInfo.printStackTrace(System.err);

        return null; // no error report, no logName
    }

    @Override
    public String reportError(Level logLevel, Throwable errorInfo, AccessContext accCtx, Peer client, String contextInfo)
    {
        System.err.println("AccCtx: Identity      : " + accCtx.subjectId.name.value + "\n" +
            "        SecurityDomain: " + accCtx.subjectDomain.name.value + "\n" +
            "        Role          :" + accCtx.subjectRole.name.value
        );
        System.err.println("Peer id: " + client.getId());
        System.err.println(contextInfo);
        errorInfo.printStackTrace(System.err);

        return null; // no error report, no logName
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
        System.err.println("AccCtx: Identity      : " + accCtx.subjectId.name.value + "\n" +
            "        SecurityDomain: " + accCtx.subjectDomain.name.value + "\n" +
            "        Role          :" + accCtx.subjectRole.name.value
        );
        System.err.println("Peer id: " + client.getId());
        System.err.println(contextInfo);
        errorInfo.printStackTrace(System.err);

        return null; // no error report, no logName
    }

    @Override
    public void setTraceEnabled(AccessContext accCtx, boolean flag) throws AccessDeniedException
    {
        // Tracing on/off not implemented, no-op
    }
}
