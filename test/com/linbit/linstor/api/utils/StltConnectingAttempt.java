package com.linbit.linstor.api.utils;

import com.linbit.linstor.Node;
import com.linbit.linstor.netcom.TcpConnector;

import java.net.InetSocketAddress;

public class StltConnectingAttempt
{
    public InetSocketAddress inetSocketAddress;
    public TcpConnector tcpConnector;
    public Node node;

    public StltConnectingAttempt(InetSocketAddress inetSocketAddress, TcpConnector tcpConnector, Node node)
    {
        this.inetSocketAddress = inetSocketAddress;
        this.tcpConnector = tcpConnector;
        this.node = node;
    }
}
