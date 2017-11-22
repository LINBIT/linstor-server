package com.linbit.linstor.testutils;

import org.slf4j.event.Level;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

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
        // Hex instance ID of linstor's error reporter
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
        Level logLevel, LinStorException errorInfo, AccessContext accCtx, Peer client, String contextInfo
    )
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
        return null; // no error report, no logName
    }

    @Override
    public void setTraceEnabled(AccessContext accCtx, boolean flag) throws AccessDeniedException
    {
        // Tracing on/off not implemented, no-op
    }
}