package com.linbit.linstor.storage.interfaces.layers.drbd;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.interfaces.categories.RscDfnLayerObject;
import com.linbit.linstor.TcpPortNumber;

public interface DrbdRscDfnObject extends RscDfnLayerObject
{
    TcpPortNumber getPort();

    TransportType getTransportType();

    String getSecret();
}
