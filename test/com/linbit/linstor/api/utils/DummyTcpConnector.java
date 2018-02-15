package com.linbit.linstor.api.utils;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.linbit.ServiceName;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.netcom.TcpConnectorPeer;

public class DummyTcpConnector implements TcpConnector
{
    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        // no-op
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        // no-op
    }

    @Override
    public void shutdown()
    {
        // no-op
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        // no-op
    }

    @Override
    public ServiceName getServiceName()
    {
        return null;
    }

    @Override
    public String getServiceInfo()
    {
        return null;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return null;
    }

    @Override
    public boolean isStarted()
    {
        return false;
    }

    @Override
    public Peer connect(InetSocketAddress address, Node node) throws IOException
    {
        // no-op
        return null;
    }

    @Override
    public Peer reconnect(Peer peer) throws IOException
    {
        // no-op
        return null;
    }

    @Override
    public void closeConnection(TcpConnectorPeer peerObj, boolean allowReconnect)
    {
        // no-op
    }

    @Override
    public void wakeup()
    {
        // no-op
    }
}
