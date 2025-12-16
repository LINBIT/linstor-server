package com.linbit.linstor.logging;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.io.PrintStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.event.Level;

public class StderrErrorReporter extends BaseErrorReporter implements ErrorReporter
{
    private final AtomicLong errorNr = new AtomicLong(0L);

    public StderrErrorReporter(String moduleName)
    {
        super(moduleName, false, "");
    }

    @Override
    public boolean hasAtLeastLogLevel(Level leveRef)
    {
        return true;
    }

    @Override
    public @Nullable Level getCurrentLogLevel()
    {
        return Level.TRACE;
    }

    @Override
    public void setLogLevel(@Nullable AccessContext accCtx, @Nullable Level levelRef, @Nullable Level linstorLevelRef)
    {
    }

    @Override
    public void logTrace(String format, Object... args)
    {
        System.err.println("TRACE:   " + String.format(format, args));
    }

    @Override
    public void logDebug(String format, Object... args)
    {
        System.err.println("DEBUG:   " + String.format(format, args));
    }

    @Override
    public void logInfo(String format, Object... args)
    {
        System.err.println("INFO:    " + String.format(format, args));
    }

    @Override
    public void logWarning(String format, Object... args)
    {
        System.err.println("WARNING: " + String.format(format, args));
    }

    @Override
    public void logError(String format, Object... args)
    {
        System.err.println("ERROR:   " + String.format(format, args));
    }

    @Override
    public String getInstanceId()
    {
        return "AFFE00";
    }

    @Override
    public @Nullable String reportError(Throwable errorInfo)
    {
        return reportError(Level.ERROR, errorInfo, null, null, null);
    }

    @Override
    public @Nullable String reportError(Level logLevel, Throwable errorInfo)
    {
        return reportError(logLevel, errorInfo, null, null, null);
    }

    @Override
    public @Nullable String reportError(
        Throwable errorInfo,
        @Nullable AccessContext accCtx,
        @Nullable Peer client,
        String contextInfo
    )
    {
        return reportImpl(errorInfo, accCtx, client, contextInfo, true);
    }

    @Override
    public @Nullable String reportError(
        Level logLevel,
        Throwable errorInfo,
        @Nullable AccessContext accCtx,
        @Nullable Peer client,
        @Nullable String contextInfo
    )
    {
        return reportImpl(errorInfo, accCtx, client, contextInfo, true);
    }

    @Override
    public @Nullable String reportProblem(
        Level logLevel,
        LinStorException errorInfo,
        @Nullable AccessContext accCtx,
        @Nullable Peer client,
        String contextInfo
    )
    {
        return reportImpl(errorInfo, accCtx, client, contextInfo, false);
    }

    private @Nullable String reportImpl(
        Throwable errorInfoRef,
        @Nullable AccessContext accCtx,
        @Nullable Peer client,
        @Nullable String contextInfo,
        boolean includeStackTrace
    )
    {
        PrintStream output = System.err;
        String logName = null;
        final LocalDateTime errorTime = LocalDateTime.now();
        try
        {
            long reportNr = errorNr.getAndIncrement();

            Throwable errorInfo = errorInfoRef;
            // Generate and report a null pointer exception if this
            // method is called with a null argument
            if (errorInfo == null)
            {
                errorInfo = new NullPointerException();
            }

            // Error report header
            ErrorReportRenderer errRepRenderer = new ErrorReportRenderer();

            renderReport(
                errRepRenderer,
                reportNr,
                accCtx,
                client,
                errorInfo,
                errorTime,
                contextInfo,
                includeStackTrace
            );

            String renderedReport = errRepRenderer.getErrorReport();

            output.print(renderedReport);
        }
        catch (Exception exc)
        {
            exc.printStackTrace(System.err);
        }
        return logName;
    }

    @Override
    public Path getLogDirectory()
    {
        return Paths.get("/tmp");
    }
}
