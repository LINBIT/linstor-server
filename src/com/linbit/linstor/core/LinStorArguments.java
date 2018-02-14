package com.linbit.linstor.core;

public class LinStorArguments
{
    private String workingDirectory;

    private boolean startDebugConsole;

    private String memoryDatabaseInit;

    public LinStorArguments()
    {
        workingDirectory = "";
        memoryDatabaseInit = null;
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

    public void setMemoryDatabaseInitScript(final String memoryDatabaseInitScript )
    {
        this.memoryDatabaseInit = memoryDatabaseInitScript;
    }

    public String getMemoryDatabaseInitScript() { return memoryDatabaseInit; }
}
