package com.linbit.linstor.api.protobuf.satellite;

import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.core.Satellite;

public class UpdateRequester
{
    private final Satellite satellite;

    public UpdateRequester(Satellite satelliteRef)
    {
        satellite = satelliteRef;
    }

    public void requestResourceUpdate(
        UUID rscUuid,
        ResourceName rscName,
        Map<NodeName, UUID> nodeIds
    )
    {
        satellite.getControllerPeer();
    }
}
