/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.api.pojo;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.VolumeDefinition;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class RscDfnPojo implements ResourceDefinition.RscDfnApi {

    private final UUID uuid;
    private final String name;
    private final int port;
    private final String secret;
    private final long flags;
    private final Map<String, String> props;
    private final List<VolumeDefinition.VlmDfnApi> vlmdfns;

    public RscDfnPojo(
            final UUID uuid,
            final String name,
            final int port,
            final String secret,
            final long flags,
            final Map<String, String> props,
            final List<VolumeDefinition.VlmDfnApi> vlmdfns)
    {
        this.uuid = uuid;
        this.name = name;
        this.port = port;
        this.secret = secret;
        this.flags = flags;
        this.props = props;
        this.vlmdfns = vlmdfns;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public String getResourceName() {
        return name;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getSecret() {
        return secret;
    }

    @Override
    public long getFlags() {
        return flags;
    }

    @Override
    public Map<String, String> getProps() {
        return props;
    }

    @Override
    public List<VolumeDefinition.VlmDfnApi> getVlmDfnList() {
        return vlmdfns;
    }

}
