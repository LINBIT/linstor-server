package com.linbit.linstor.core;

public class LinStorArguments
{
    private String workingDirectory;

    private boolean startDebugConsole;

    private String inMemoryDbType;
    private int inMemoryDbPort;
    private String inMemoryDbAddress;

    public LinStorArguments()
    {
        workingDirectory = "";
        inMemoryDbType = null;
        inMemoryDbAddress = null;
        inMemoryDbPort = 0;
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

    public void setInMemoryDbType(final String inMemoryDbType)
    {
        this.inMemoryDbType = inMemoryDbType;
    }

    public void setInMemoryDbPort(final int inMemoryDbPort)
    {
        this.inMemoryDbPort = inMemoryDbPort;
    }

    public void setInMemoryDbAddress(final String inMemoryDbAddress)
    {
        this.inMemoryDbAddress = inMemoryDbAddress;
    }

    public String getInMemoryDbType()
    {
        return inMemoryDbType;
    }

    public int getInMemoryDbPort()
    {
        return inMemoryDbPort;
    }

    public String getInMemoryDbAddress()
    {
        return inMemoryDbAddress;
    }
}
