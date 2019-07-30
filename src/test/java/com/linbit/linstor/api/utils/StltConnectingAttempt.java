package com.linbit.linstor.api.utils;

import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.TcpConnector;

import java.net.InetSocketAddress;

public class StltConnectingAttempt
{
    public InetSocketAddress inetSocketAddress;
    public TcpConnector tcpConnector;
    public Node node;

    public StltConnectingAttempt(
        InetSocketAddress inetSocketAddressRef,
        TcpConnector tcpConnectorRef,
        Node nodeRef
    )
    {
        inetSocketAddress = inetSocketAddressRef;
        tcpConnector = tcpConnectorRef;
        node = nodeRef;
    }
}
