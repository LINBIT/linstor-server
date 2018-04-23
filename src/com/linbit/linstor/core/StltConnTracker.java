package com.linbit.linstor.core;

import com.linbit.linstor.SatellitePeerCtx;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class StltConnTracker implements ConnectionObserver
{
    private final CoreModule.PeerMap peerMap;
    private final EventBroker eventBroker;

    @Inject
    StltConnTracker(
        CoreModule.PeerMap peerMapRef,
        EventBroker eventBrokerRef
    )
    {
        peerMap = peerMapRef;
        eventBroker = eventBrokerRef;
    }

    @Override
    public void outboundConnectionEstablished(Peer connPeer)
    {
        // FIXME: Something should done here for completeness, although the Satellite
        //        does not normally connect outbound
        if (connPeer != null)
        {
            SatellitePeerCtx peerCtx = (SatellitePeerCtx) connPeer.getAttachment();
            if (peerCtx == null)
            {
                peerCtx = new SatellitePeerCtx();
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
            SatellitePeerCtx peerCtx = new SatellitePeerCtx();
            connPeer.attach(peerCtx);
            synchronized (peerMap)
            {
                peerMap.put(connPeer.getId(), connPeer);
            }
        }
    }

    @Override
    public void connectionClosed(Peer connPeer, boolean allowReconnect)
    {
        if (connPeer != null)
        {
            eventBroker.removeWatchesForPeer(connPeer.getId());

            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
            }
        }
    }
}
