package com.linbit.linstor.api.utils;

import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;

public class SatelliteConnectionApiTestImpl implements SatelliteConnectionApi
{
    private String netIfName;
    private int port;
    private String encryptionType;

    public SatelliteConnectionApiTestImpl(String netIfNameRef, int portRef, String encryptionTypeRef)
    {
        netIfName = netIfNameRef;
        port = portRef;
        encryptionType = encryptionTypeRef;
    }

    @Override
    public String getNetInterfaceName()
    {
        return netIfName;
    }

    @Override
    public int getPort()
    {
        return port;
    }

    @Override
    public String getEncryptionType()
    {
        return encryptionType;
    }

}
