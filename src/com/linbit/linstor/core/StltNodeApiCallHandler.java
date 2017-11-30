package com.linbit.linstor.core;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.interfaces.serializer.StltRequestSerializer;
import com.linbit.linstor.security.AccessContext;

public class StltNodeApiCallHandler
{
    private Satellite satellite;
    private AccessContext apiCtx;
    private StltRequestSerializer<NodeName> nodeRequestSerializer;

    public StltNodeApiCallHandler(Satellite satelliteRef, AccessContext apiCtxRef, StltRequestSerializer<NodeName> nodeRequestSerializer)
    {
        satellite = satelliteRef;
        apiCtx = apiCtxRef;
        this.nodeRequestSerializer = nodeRequestSerializer;
    }

}
