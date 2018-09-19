package com.linbit.linstor.core;

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
        logDirectory = "";
    }

    public void setConfigurationDirectory(final String workingDirectoryRef)
    {
        configurationDirectory = workingDirectoryRef;
    }

    public String getConfigurationDirectory()
    {
        return configurationDirectory;
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

    public void setLogDirectory(String logDirectory)
    {
        this.logDirectory = logDirectory;
    }
}
