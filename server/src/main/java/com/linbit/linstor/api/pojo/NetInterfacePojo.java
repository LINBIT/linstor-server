package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.NetInterfaceApi;

import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NetInterfacePojo implements NetInterfaceApi
{
    private final UUID uuid;
    private final String name;
    private final String address;
    private final Integer port;
    private final String encrType;

    public NetInterfacePojo(
        final UUID uuidRef,
        final String nameRef,
        final String addressRef,
        final Integer portRef,
        final String encrTypeRef
    )
    {
        uuid = uuidRef;
        name = nameRef;
        address = addressRef;
        port = portRef;
        encrType = encrTypeRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public String getAddress()
    {
        return address;
    }

    @Override
    public boolean isUsableAsSatelliteConnection()
    {
        return port != null && encrType != null;
    }

    @Override
    public int getSatelliteConnectionPort()
    {
        return port;
    }

    @Override
    public String getSatelliteConnectionEncryptionType()
    {
        return encrType;
    }
}
