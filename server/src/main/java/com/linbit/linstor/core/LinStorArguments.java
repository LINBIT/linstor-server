package com.linbit.linstor.core;

import java.util.regex.Pattern;

public class LinStorArguments
{
    private String configurationDirectory;

    private boolean startDebugConsole;

    private String inMemoryDbType;
    private int inMemoryDbPort;
    private String inMemoryDbAddress;
    private boolean printStacktraces;
    private String logDirectory;
    private Pattern keepResPattern;

    public LinStorArguments()
    {
        configurationDirectory = "";
        inMemoryDbType = null;
        inMemoryDbAddress = null;
        inMemoryDbPort = 0;
        printStacktraces = false;
        logDirectory = "";
        keepResPattern = null;
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

    public Pattern getKeepResPattern()
    {
        return keepResPattern;
    }

    public void setKeepResRegex(String keepResRegex)
    {
        keepResPattern = Pattern.compile(keepResRegex, Pattern.DOTALL);
    }
}
