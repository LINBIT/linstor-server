package com.linbit.linstor.core;

import java.io.IOException;
import java.util.Map;

import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.logging.BaseErrorReporter;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
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

    private void sendApiVersionMessage(final Peer connPeer)
    {
        StringBuilder controllerInfo = new StringBuilder();
        controllerInfo.append(LinStor.PROGRAM).append(',')
            .append("Controller").append(',')
            .append(LinStor.VERSION).append(',');
        if (BaseErrorReporter.CURRENT_GIT_HASH != null)
            controllerInfo.append(BaseErrorReporter.CURRENT_GIT_HASH.trim());

        byte[] data = controller.getApiCallHandler().getCtrlClientcomSrzl()
            .builder(ApiConsts.API_VERSION, 0)
            .apiVersion(0, controllerInfo.toString())
            .build();

        Message msg = connPeer.createMessage();
        try {
            msg.setData(data);
            connPeer.sendMessage(msg);
        }
        catch (IllegalMessageStateException illState) {
            controller.getErrorReporter().reportError(illState);
        }
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