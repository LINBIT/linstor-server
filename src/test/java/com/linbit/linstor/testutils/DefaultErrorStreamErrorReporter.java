package com.linbit.linstor.testutils;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import javax.annotation.Nullable;

import java.nio.file.Path;

import org.slf4j.event.Level;

public class DefaultErrorStreamErrorReporter implements ErrorReporter
{
    @Override
    public String getInstanceId()
    {
        // Hex instance ID of linstor's error reporter
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
    public String reportError(Throwable errorInfo, @Nullable AccessContext accCtx, Peer client, String contextInfo)
    {
        if (accCtx != null)
        {
            System.err.println(
                "AccCtx: Identity      : " + accCtx.subjectId.name.value + "\n" +
                    "        SecurityDomain: " + accCtx.subjectDomain.name.value + "\n" +
                    "        Role          :" + accCtx.subjectRole.name.value
            );
        }
        System.err.println("Peer id: " + client);
        System.err.println(contextInfo);
        errorInfo.printStackTrace(System.err);

        return null; // no error report, no logName
    }

    @Override
    public String reportError(
        Level logLevel,
        Throwable errorInfo,
        @Nullable AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        if (accCtx != null)
        {
            System.err.println(
                "AccCtx: Identity      : " + accCtx.subjectId.name.value + "\n" +
                    "        SecurityDomain: " + accCtx.subjectDomain.name.value + "\n" +
                    "        Role          :" + accCtx.subjectRole.name.value
            );
        }
        System.err.println("Peer id: " + client);
        System.err.println(contextInfo);
        errorInfo.printStackTrace(System.err);

        return null; // no error report, no logName
    }

    @Override
    public String reportProblem(
        Level logLevel,
        LinStorException errorInfo,
        @Nullable AccessContext accCtx,
        Peer client,
        String contextInfo
    )
    {
        if (accCtx != null)
        {
            System.err.println(
                "AccCtx: Identity      : " + accCtx.subjectId.name.value + "\n" +
                    "        SecurityDomain: " + accCtx.subjectDomain.name.value + "\n" +
                    "        Role          :" + accCtx.subjectRole.name.value
            );
        }
        System.err.println("Peer id: " + client);
        System.err.println(contextInfo);
        errorInfo.printStackTrace(System.err);

        return null; // no error report, no logName
    }

    @Override
    public void setLogLevel(@Nullable AccessContext accCtx, @Nullable Level levelRef, @Nullable Level linstorLevelRef)
    {
        // Tracing on/off not implemented, no-op
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
