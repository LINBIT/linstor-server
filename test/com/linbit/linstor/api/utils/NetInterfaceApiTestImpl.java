package com.linbit.linstor.api.utils;

import java.util.UUID;

import com.linbit.linstor.NetInterface.NetInterfaceApi;

public class NetInterfaceApiTestImpl implements NetInterfaceApi
{
    private UUID uuid;
    private String name;
    private String address;

    public NetInterfaceApiTestImpl(UUID uuidRef, String nameRef, String addressRef)
    {
        uuid = uuidRef;
        name = nameRef;
        address = addressRef;
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

}
