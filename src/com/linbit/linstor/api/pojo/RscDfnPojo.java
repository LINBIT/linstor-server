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
public class RscDfnPojo implements ResourceDefinition.RscDfnApi
{

    private final UUID uuid;
    private final String name;
    private final int port;
    private final String secret;
    private final long flags;
    private final String transportType;
    private final Map<String, String> props;
    private final List<VolumeDefinition.VlmDfnApi> vlmdfns;

    public RscDfnPojo(
        final UUID uuidRef,
        final String nameRef,
        final int portRef,
        final String secretRef,
        final long flagsRef,
        final String transportTypeRef,
        final Map<String, String> propsRef,
        final List<VolumeDefinition.VlmDfnApi> vlmdfnsRef
    )
    {
        uuid = uuidRef;
        name = nameRef;
        port = portRef;
        secret = secretRef;
        flags = flagsRef;
        transportType = transportTypeRef;
        props = propsRef;
        vlmdfns = vlmdfnsRef;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public String getResourceName()
    {
        return name;
    }

    @Override
    public int getPort()
    {
        return port;
    }

    @Override
    public String getSecret()
    {
        return secret;
    }

    @Override
    public long getFlags()
    {
        return flags;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }

    @Override
    public List<VolumeDefinition.VlmDfnApi> getVlmDfnList()
    {
        return vlmdfns;
    }

    @Override
    public String getTransportType()
    {
        return transportType;
    }
}
