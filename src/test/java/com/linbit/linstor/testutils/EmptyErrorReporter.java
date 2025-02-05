package com.linbit.linstor.testutils;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.nio.file.Path;

import org.slf4j.event.Level;

public class EmptyErrorReporter implements ErrorReporter
{
    private boolean printStacktraces;

    public EmptyErrorReporter()
    {
        this(true);
    }

    public EmptyErrorReporter(boolean printStacktracesRef)
    {
        printStacktraces = printStacktracesRef;
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
    public String reportError(
        Level logLevel,
        Throwable errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
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
        Level logLevel,
        LinStorException errorInfo,
        AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        if (printStacktraces)
        {
            errorInfo.printStackTrace();
        }
        return null; // no error report, no logName
    }

    @Override
    public void setLogLevel(@Nullable AccessContext accCtx, @Nullable Level levelRef, @Nullable Level linstorLevelRef)
    {
    }

    @Override
    public Level getCurrentLogLevel()
    {
        return Level.TRACE;
    }

    @Override
    public boolean hasAtLeastLogLevel(Level leveRef)
    {
        return true;
    }

    @Override
    public Path getLogDirectory()
    {
        return null;
    }
}
