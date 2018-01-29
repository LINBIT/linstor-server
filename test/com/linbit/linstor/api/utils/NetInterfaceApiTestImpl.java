package com.linbit.linstor.api.utils;

import java.util.UUID;

import com.linbit.linstor.NetInterface.NetInterfaceApi;

public class NetInterfaceApiTestImpl implements NetInterfaceApi
{
    private UUID uuid;
    private String name;
    private String address;

    public NetInterfaceApiTestImpl(UUID uuid, String name, String address)
    {
        this.uuid = uuid;
        this.name = name;
        this.address = address;
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
