package com.linbit.linstor.storage.layer.data;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.storage.layer.data.categories.RscDfnLayerData;
import com.linbit.linstor.TcpPortNumber;

public interface DrbdRscDfnData extends RscDfnLayerData
{
    TcpPortNumber getPort();

    TransportType getTransportType();

    String getSecret();
}
