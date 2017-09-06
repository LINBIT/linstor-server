package com.linbit.drbdmanage.core;

import java.util.Map;

import com.linbit.drbdmanage.SatellitePeerCtx;
import com.linbit.drbdmanage.netcom.ConnectionObserver;
import com.linbit.drbdmanage.netcom.Peer;

class StltConnTracker implements ConnectionObserver
{
    private final Satellite satellite;
    private final Map<String, Peer> peerMap;

    StltConnTracker(Satellite satelliteRef, Map<String, Peer> peerMapRef)
    {
        satellite = satelliteRef;
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