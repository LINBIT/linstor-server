package com.linbit.linstor.netcom;

import com.linbit.ServiceName;
import com.linbit.linstor.annotation.Nullable;

public interface NetComContainer
{
    @Nullable
    TcpConnector getNetComConnector(ServiceName conSvcName);

    void removeNetComConnector(ServiceName conSvnName);

    void putNetComContainer(ServiceName conSvcName, TcpConnector netComSvc);
}
