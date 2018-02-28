package com.linbit.linstor.core;

import com.linbit.linstor.SatellitePeerCtx;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;

class StltConnTracker implements ConnectionObserver
{
    private final CoreModule.PeerMap peerMap;

    StltConnTracker(CoreModule.PeerMap peerMapRef)
    {
        peerMap = peerMapRef;
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
            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
            }
        }
    }
}
