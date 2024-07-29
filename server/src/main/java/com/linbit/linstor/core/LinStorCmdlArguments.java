package com.linbit.linstor.core;

import com.linbit.linstor.annotation.Nullable;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LinStorCmdlArguments
{
    private @Nullable String configurationDirectory;
    private @Nullable Boolean startDebugConsole;
    private @Nullable Boolean printStacktraces;
    private @Nullable String logDirectory;
    private @Nullable String logLevel;
    private @Nullable Boolean dbStartupVerification;

    public LinStorCmdlArguments()
    {
    }

    public void setConfigurationDirectory(final String workingDirectoryRef)
    {
        configurationDirectory = workingDirectoryRef;
    }

    /**
     * Returns the absolute config directory path.
     * @return Path to the configuration directory
     */
    public Path getConfigurationDirectory()
    {
        return Paths.get(configurationDirectory).toAbsolutePath();
    }

    public void setStartDebugConsole(final boolean startDebugConsoleRef)
    {
        startDebugConsole = startDebugConsoleRef;
    }

    public @Nullable Boolean startDebugConsole()
    {
        return startDebugConsole;
    }

    public @Nullable Boolean isPrintStacktraces()
    {
        return printStacktraces;
    }

    public void setPrintStacktraces(boolean printStacktracesRef)
    {
        printStacktraces = printStacktracesRef;
    }

    public @Nullable String getLogDirectory()
    {
        return logDirectory;
    }

    public void setLogDirectory(String newLogDirectory)
    {
        logDirectory = newLogDirectory;
    }

    public @Nullable String getLogLevel()
    {
        return logLevel;
    }

    public void setLogLevel(String logLevelRef)
    {
        logLevel = logLevelRef;
    }

    public void setDbStartupVerification(boolean dbStartupVerificationRef)
    {
        dbStartupVerification = dbStartupVerificationRef;
    }

    public @Nullable Boolean isDbStartupVerification()
    {
        return dbStartupVerification;
    }

}
