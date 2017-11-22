package com.linbit.linstor.core;

import java.io.IOException;
import java.util.Map;

import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.ReconnectorTask;

class CtrlConnTracker implements ConnectionObserver
{
    private final Controller controller;
    private final Map<String, Peer> peerMap;
    private final ReconnectorTask reconnectorTask;

    CtrlConnTracker(
        Controller controllerRef,
        Map<String, Peer> peerMapRef,
        ReconnectorTask reconnectorTaskRef
    )
    {
        controller = controllerRef;
        peerMap = peerMapRef;
        reconnectorTask = reconnectorTaskRef;
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
            reconnectorTask.peerConnected(connPeer);
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