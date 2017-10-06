package com.linbit.drbdmanage.testutils;

import org.slf4j.event.Level;

import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

public class EmptyErrorReporter implements ErrorReporter
{
    private boolean printStacktraces;

    public EmptyErrorReporter()
    {
        this(true);
    }

    public EmptyErrorReporter(boolean printStacktraces)
    {
        this.printStacktraces = printStacktraces;
    }

    @Override
    public String getInstanceId()
    {
        // Hex instance ID of drbdmanageNG's error reporter
        // Not significant for the test, just needs to return something to implement the interface
        return "CAFEAFFE";
    }

    @Override
    public String reportError(Level logLevel, Throwable errorInfo)
    {
        // ignore
        return null; // no error report, no logName
    }

    @Override
    public String reportError(Level logLevel, Throwable errorInfo, AccessContext accCtx, Peer client, String contextInfo)
    {
        // ignore
        return null; // no error report, no logName
    }

    @Override
    public void logTrace(String format, Object... args)
    {
        // ignore
    }

    @Override
    public void logDebug(String format, Object... args)
    {
        // ignore
    }

    @Override
    public void logInfo(String format, Object... args)
    {
        // ignore
    }

    @Override
    public void logWarning(String format, Object... args)
    {
        // ignore
    }

    @Override
    public void logError(String format, Object... args)
    {
        // ignore
    }

    @Override
    public String reportError(Throwable errorInfo)
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
        return null; // no error report, no logName
    }

    @Override
    public String reportError(Throwable errorInfo, AccessContext accCtx, Peer client, String contextInfo)
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
        return null; // no error report, no logName
    }

    @Override
    public String reportProblem(
        Level logLevel, DrbdManageException errorInfo, AccessContext accCtx, Peer client, String contextInfo
    )
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
        return null; // no error report, no logName
    }

    @Override
    public String getInstanceId()
    {
        return "EmptyErrorReporter";
    }
}