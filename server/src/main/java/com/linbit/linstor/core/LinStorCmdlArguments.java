package com.linbit.linstor.core;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LinStorCmdlArguments
{
    private String configurationDirectory;
    private Boolean startDebugConsole;
    private Boolean printStacktraces;
    private String logDirectory;
    private String logLevel;
    private Boolean dbStartupVerification;

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

    public Boolean startDebugConsole()
    {
        return startDebugConsole;
    }

    public Boolean isPrintStacktraces()
    {
        return printStacktraces;
    }

    public void setPrintStacktraces(boolean printStacktracesRef)
    {
        printStacktraces = printStacktracesRef;
    }

    public String getLogDirectory()
    {
        return logDirectory;
    }

    public void setLogDirectory(String newLogDirectory)
    {
        logDirectory = newLogDirectory;
    }

    public String getLogLevel()
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

    public Boolean isDbStartupVerification()
    {
        return dbStartupVerification;
    }

}
