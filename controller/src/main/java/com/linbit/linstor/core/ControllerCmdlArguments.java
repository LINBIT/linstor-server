package com.linbit.linstor.core;

public class ControllerCmdlArguments extends LinStorCmdlArguments
{
    private String inMemoryDbType;
    private int inMemoryDbPort;
    private String inMemoryDbAddress;
    private String restBindAddress;
    private String restBindAddressSecure;
    private String logLevel;

    public ControllerCmdlArguments()
    {
        inMemoryDbType = null;
        inMemoryDbAddress = null;
        inMemoryDbPort = 0;
        restBindAddress = null;
        logLevel = null;
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

    public String getRESTBindAddress()
    {
        return restBindAddress;
    }

    public String getRESTBindAddressSecure()
    {
        return restBindAddressSecure;
    }

    public void setRESTBindAddress(String restBindAddressRef)
    {
        restBindAddress = restBindAddressRef;
    }
    public void setRESTBindAddressSecure(String restBindAddressRef)
    {
        restBindAddressSecure = restBindAddressRef;
    }

    public String getLogLevel()
    {
        return logLevel;
    }

    public void setLogLevel(String logLevelRef)
    {
        logLevel = logLevelRef;
    }
}
