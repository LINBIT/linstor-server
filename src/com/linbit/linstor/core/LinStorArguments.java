package com.linbit.linstor.core;

public class LinStorArguments
{
    private String workingDirectory;

    private boolean startDebugConsole;

    private String inMemoryDbType;
    private int inMemoryDbPort;
    private String inMemoryDbAddress;
    private boolean printStacktraces;

    public LinStorArguments()
    {
        workingDirectory = "";
        inMemoryDbType = null;
        inMemoryDbAddress = null;
        inMemoryDbPort = 0;
        printStacktraces = false;
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

    public void setInMemoryDbType(final String inMemoryDbTypeRef)
    {
        inMemoryDbType = inMemoryDbTypeRef;
    }

    public void setInMemoryDbPort(final int inMemoryDbPortRef)
    {
        inMemoryDbPort = inMemoryDbPortRef;
    }

    public void setInMemoryDbAddress(final String inMemoryDbAddressRef)
    {
        inMemoryDbAddress = inMemoryDbAddressRef;
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

    public boolean isPrintStacktraces()
    {
        return printStacktraces;
    }

    public void setPrintStacktraces(boolean printStacktraces)
    {
        this.printStacktraces = printStacktraces;
    }
}
