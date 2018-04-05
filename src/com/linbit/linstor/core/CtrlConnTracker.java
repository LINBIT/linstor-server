package com.linbit.linstor.core;

import java.io.IOException;

import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.ReconnectorTask;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
class CtrlConnTracker implements ConnectionObserver
{
    private final CtrlClientSerializer ctrlClientSerializer;
    private final CoreModule.PeerMap peerMap;
    private final ReconnectorTask reconnectorTask;

    @Inject
    CtrlConnTracker(
        CtrlClientSerializer ctrlClientSerializerRef,
        CoreModule.PeerMap peerMapRef,
        ReconnectorTask reconnectorTaskRef
    )
    {
        ctrlClientSerializer = ctrlClientSerializerRef;
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

    private void sendApiVersionMessage(final Peer connPeer)
    {
        StringBuilder controllerInfo = new StringBuilder();
        controllerInfo.append(LinStor.PROGRAM).append(',')
            .append("Controller").append(',')
            .append(LinStor.VERSION_INFO_PROVIDER.getVersion()).append(',')
            .append(LinStor.VERSION_INFO_PROVIDER.getGitCommitId()).append(',')
            .append(LinStor.VERSION_INFO_PROVIDER.getBuildTime());

        connPeer.sendMessage(
            ctrlClientSerializer
                .builder(ApiConsts.API_VERSION, 0)
                .apiVersion(0, controllerInfo.toString())
                .build()
        );
    }

    @Override
    public void inboundConnectionEstablished(Peer connPeer)
    {
        if (connPeer != null)
        {
            ControllerPeerCtx peerCtx = new ControllerPeerCtx();
            connPeer.attach(peerCtx);
            sendApiVersionMessage(connPeer);
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
                if (!allowReconnect)
                {
                    reconnectorTask.removePeer(connPeer);
                }
            }
        }
    }
}
