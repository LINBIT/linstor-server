package com.linbit.linstor.storage2.layer.data;

import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.storage2.layer.data.categories.RscDfnLayerData;

public interface DrbdRscDfnData extends RscDfnLayerData
{
    TcpPortNumber getPort();

    TransportType getTransportType();

    String getSecret();
}
