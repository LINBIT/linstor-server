package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apis.NetInterfaceApi;

import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NetInterfacePojo implements NetInterfaceApi
{
    private final @Nullable UUID uuid;
    private final String name;
    private final String address;
    private final @Nullable Integer port;
    private final @Nullable String encrType;

    public NetInterfacePojo(
        final @Nullable UUID uuidRef,
        final String nameRef,
        final String addressRef,
        final @Nullable Integer portRef,
        final @Nullable String encrTypeRef
    )
    {
        uuid = uuidRef;
        name = nameRef;
        address = addressRef;
        port = portRef;
        encrType = encrTypeRef;
    }

    @Override
    public @Nullable UUID getUuid()
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
    public @Nullable Integer getSatelliteConnectionPort()
    {
        return port;
    }

    @Override
    public @Nullable String getSatelliteConnectionEncryptionType()
    {
        return encrType;
    }
}
