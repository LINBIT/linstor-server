package com.linbit.linstor.core;

import com.linbit.linstor.Node;
import com.linbit.linstor.netcom.TcpConnector;

import java.net.InetSocketAddress;

public interface SatelliteConnector
{
    void connectSatellite(
        InetSocketAddress satelliteAddress,
        TcpConnector tcpConnector,
        Node node
    );
}
