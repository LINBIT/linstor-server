package com.linbit.linstor.api.pojo;

import com.linbit.linstor.NetInterface;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NetInterfacePojo implements NetInterface.NetInterfaceApi
{
    private final UUID uuid;
    private final String name;
    private final String address;

    public NetInterfacePojo(
        final UUID uuidRef,
        final String nameRef,
        final String addressRef
    )
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
