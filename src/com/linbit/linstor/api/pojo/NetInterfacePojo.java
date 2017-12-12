/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.pojo;

import com.linbit.linstor.NetInterface;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class NetInterfacePojo implements NetInterface.NetInterfaceApi {
    private final UUID uuid;
    private final String name;
    private final String address;
    private final int port;
    private final String type;

    public NetInterfacePojo(
        final UUID uuid,
        final String name,
        final String address,
        final String type,
        final int port)
    {
        this.uuid = uuid;
        this.name = name;
        this.address = address;
        this.port = port;
        this.type = type;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getType() {
        return type;
    }


}
