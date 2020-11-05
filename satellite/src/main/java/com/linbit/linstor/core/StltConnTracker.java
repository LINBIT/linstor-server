package com.linbit.linstor.core;

import com.linbit.linstor.SatellitePeerCtx;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.Objects;

@Singleton
class StltConnTracker implements ConnectionObserver
{
    private final CoreModule.PeerMap peerMap;
    private final EventBroker eventBroker;
    private final ControllerPeerConnector controllerPeerConnector;
    private final DeviceManager devMgr;

    @Inject
    StltConnTracker(
        CoreModule.PeerMap peerMapRef,
        EventBroker eventBrokerRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        DeviceManager devMgrRef
    )
    {
        peerMap = peerMapRef;
        eventBroker = eventBrokerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        devMgr = devMgrRef;
    }

    @Override
    public void outboundConnectionEstablished(Peer connPeer)
    {
        // FIXME: Something should done here for completeness, although the Satellite
        //        does not normally connect outbound
        addToPeerMap(connPeer);
    }

    @Override
    public void outboundConnectionEstablishing(Peer peerRef) throws IOException
    {
        addToPeerMap(peerRef);
    }

    private void addToPeerMap(Peer connPeer)
    {
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
    public void connectionClosed(Peer connPeer, boolean allowReconnect, boolean shuttingDown)
    {
        if (connPeer != null)
        {
            if (Objects.equals(connPeer, controllerPeerConnector.getControllerPeer()))
            {
                devMgr.controllerConnectionLost();
            }
            eventBroker.connectionClosed(connPeer);

            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
            }
        }
    }
}
