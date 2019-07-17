package com.linbit.linstor.core;

import java.nio.file.Path;
import java.nio.file.Paths;

public class LinStorCmdlArguments
{
    private String configurationDirectory;
    private boolean startDebugConsole;
    private boolean printStacktraces;
    private String logDirectory;

    public LinStorCmdlArguments()
    {
        configurationDirectory = "";
        printStacktraces = false;
        logDirectory = ".";
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

    public boolean startDebugConsole()
    {
        return startDebugConsole;
    }

    public boolean isPrintStacktraces()
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
}
