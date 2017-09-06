package com.linbit.drbdmanage.core;

import java.io.IOException;
import java.util.Map;

import com.linbit.drbdmanage.ControllerPeerCtx;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.netcom.TcpReconnectorService;

class CtrlConnTracker implements ConnectionObserver
{
    private final Controller controller;
    private final Map<String, Peer> peerMap;
    private final TcpReconnectorService reconnectorService;

    CtrlConnTracker(
        Controller controllerRef,
        Map<String, Peer> peerMapRef,
        TcpReconnectorService reconnectorServiceRef)
    {
        controller = controllerRef;
        peerMap = peerMapRef;
        reconnectorService = reconnectorServiceRef;
    }

    @Override
    public void outboundConnectionEstablished(Peer connPeer) throws IOException
    {
        if (connPeer != null)
        {
            ControllerPeerCtx peerCtx = (ControllerPeerCtx) connPeer.getAttachment();
            if (peerCtx == null)
            {
                peerCtx = new ControllerPeerCtx();
                connPeer.attach(peerCtx);
            }
            synchronized (peerMap)
            {
                peerMap.put(connPeer.getId(), connPeer);
            }
            reconnectorService.peerConnected(connPeer);
        }
        // TODO: If a satellite has been connected, schedule any necessary actions
    }

    @Override
    public void inboundConnectionEstablished(Peer connPeer)
    {
        if (connPeer != null)
        {
            ControllerPeerCtx peerCtx = new ControllerPeerCtx();
            connPeer.attach(peerCtx);
            synchronized (peerMap)
            {
                peerMap.put(connPeer.getId(), connPeer);
            }
        }
    }

    @Override
    public void connectionClosed(Peer connPeer)
    {
        if (connPeer != null)
        {
            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
            }
        }
    }
}