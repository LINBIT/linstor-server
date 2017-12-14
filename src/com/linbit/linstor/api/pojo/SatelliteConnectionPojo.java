package com.linbit.linstor.api.pojo;

import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;

public class SatelliteConnectionPojo implements SatelliteConnectionApi
{
    private final String netIfName;
    private final int port;
    private final String encryptionType;

    public SatelliteConnectionPojo(String netIfNameRef, int portRef, String encryptionTypeRef)
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
