package com.linbit.linstor.netcom;

import com.linbit.ServiceName;

public interface NetComContainer
{
    TcpConnector getNetComConnector(ServiceName conSvcName);

    void removeNetComConnector(ServiceName conSvnName);

    void putNetComContainer(ServiceName conSvcName, TcpConnector netComSvc);
}
