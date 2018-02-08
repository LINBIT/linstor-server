package com.linbit.linstor.core;

public class LinStorArguments
{
    private String workingDirectory;

    private boolean startDebugConsole;

    public LinStorArguments()
    {
        workingDirectory = "";
    }

    public void setWorkingDirectory(final String workingDirectoryRef)
    {
        workingDirectory = workingDirectoryRef;
    }

    public String getWorkingDirectory()
    {
        return workingDirectory;
    }

    public void setStartDebugConsole(final boolean startDebugConsoleRef)
    {
        startDebugConsole = startDebugConsoleRef;
    }

    public boolean startDebugConsole()
    {
        return startDebugConsole;
    }
}
