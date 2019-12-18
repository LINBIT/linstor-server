package com.linbit.linstor.core;

import java.util.function.Function;

public class ControllerCmdlArguments extends LinStorCmdlArguments
{
    public static final String LS_REST_BIND_ADDRESS = "LS_REST_BIND_ADDRESS";
    public static final String LS_REST_BIND_ADDRESS_SECURE = "LS_REST_BIND_ADDRESS_SECURE";

    private String inMemoryDbType;
    private int inMemoryDbPort;
    private String inMemoryDbAddress;
    private String restBindAddress;
    private String restBindAddressSecure;
    public ControllerCmdlArguments()
    {
        inMemoryDbType = null;
        inMemoryDbAddress = null;
        inMemoryDbPort = 0;
        restBindAddress = getEnv(LS_REST_BIND_ADDRESS, Function.identity());
        restBindAddressSecure = getEnv(LS_REST_BIND_ADDRESS_SECURE, Function.identity());
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
}
