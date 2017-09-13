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
    public void reportError(Throwable errorInfo)
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
    }

    @Override
    public void reportError(Throwable errorInfo, AccessContext accCtx, Peer client, String contextInfo)
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
    }

    @Override
    public void reportProblem(
        Level logLevel, DrbdManageException errorInfo, AccessContext accCtx, Peer client, String contextInfo
    )
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
    }
}