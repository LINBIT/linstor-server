package com.linbit.linstor.core;

public class LinStorArguments
{
    private String workingDirectory;

    private boolean startDebugConsole;

    public LinStorArguments() {
        this.workingDirectory = "";
    }

    public void setWorkingDirectory(final String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public String getWorkingDirectory() {
        return this.workingDirectory;
    }

    public void setStartDebugConsole(final boolean startDebugConsole)
    {
        this.startDebugConsole = startDebugConsole;
    }

    public boolean startDebugConsole()
    {
        return startDebugConsole;
    }
}
