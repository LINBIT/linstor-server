package com.linbit.linstor.core;

import java.io.IOException;

import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventProcessor;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.ReconnectorTask;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class CtrlConnTracker implements ConnectionObserver
{
    private final CoreModule.PeerMap peerMap;
    private final ReconnectorTask reconnectorTask;
    private final EventBroker eventBroker;
    private final EventProcessor eventProcessor;

    @Inject
    CtrlConnTracker(
        CoreModule.PeerMap peerMapRef,
        ReconnectorTask reconnectorTaskRef,
        EventBroker eventBrokerRef,
        EventProcessor eventProcessorRef
    )
    {
        peerMap = peerMapRef;
        reconnectorTask = reconnectorTaskRef;
        eventBroker = eventBrokerRef;
        eventProcessor = eventProcessorRef;
    }

    @Override
    public void outboundConnectionEstablished(Peer connPeer) throws IOException
    {
        addToPeerMap(connPeer);
        reconnectorTask.peerConnected(connPeer);
    }

    @Override
    public void outboundConnectionEstablishing(Peer connPeer) throws IOException
    {
        addToPeerMap(connPeer);
    }

    private void addToPeerMap(Peer connPeer)
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
        }
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
    public void connectionClosed(Peer connPeer, boolean allowReconnect, boolean shuttingDown)
    {
        if (connPeer != null)
        {
            eventBroker.connectionClosed(connPeer);

            if (!shuttingDown)
            {
                eventProcessor.connectionClosed(connPeer);
            }

            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
                if (!allowReconnect)
                {
                    reconnectorTask.removePeer(connPeer);
                }
            }
        }
    }
}
