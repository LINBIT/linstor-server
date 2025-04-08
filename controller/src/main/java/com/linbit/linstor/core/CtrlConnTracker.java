package com.linbit.linstor.core;

import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventProcessor;
import com.linbit.linstor.netcom.ConnectionObserver;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.ForceReleaseSharedLocksTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.linstor.tasks.TaskScheduleService;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;

@Singleton
class CtrlConnTracker implements ConnectionObserver
{
    private final CoreModule.PeerMap peerMap;
    private final ReconnectorTask reconnectorTask;
    private final EventBroker eventBroker;
    private final EventProcessor eventProcessor;
    private final TaskScheduleService taskScheduler;
    private final NodeInternalCallHandler nodeInternalCallHandler;
    private final SharedStorPoolManager sharedSpMgr;
    private final CtrlBackupCreateApiCallHandler backupCrtApiCallHandler;

    @Inject
    CtrlConnTracker(
        CoreModule.PeerMap peerMapRef,
        ReconnectorTask reconnectorTaskRef,
        EventBroker eventBrokerRef,
        EventProcessor eventProcessorRef,
        TaskScheduleService taskSchedulerRef,
        NodeInternalCallHandler nodeInternalCallHandlerRef,
        SharedStorPoolManager sharedSpMgrRef,
        CtrlBackupCreateApiCallHandler backupCrtApiCallHandlerRef
    )
    {
        peerMap = peerMapRef;
        reconnectorTask = reconnectorTaskRef;
        eventBroker = eventBrokerRef;
        eventProcessor = eventProcessorRef;
        taskScheduler = taskSchedulerRef;
        nodeInternalCallHandler = nodeInternalCallHandlerRef;
        sharedSpMgr = sharedSpMgrRef;
        backupCrtApiCallHandler = backupCrtApiCallHandlerRef;
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
    public void connectionClosed(Peer connPeer, boolean shuttingDown)
    {
        if (connPeer != null)
        {
            eventBroker.connectionClosed(connPeer);

            if (!shuttingDown)
            {
                eventProcessor.connectionClosed(connPeer);
                backupCrtApiCallHandler.deleteNodeQueue(connPeer);
            }

            synchronized (peerMap)
            {
                peerMap.remove(connPeer.getId());
                if (!connPeer.isAllowReconnect())
                {
                    reconnectorTask.removePeer(connPeer);
                }
            }
            if (
                connPeer.isConnected(false) &&
                    connPeer.getNode() != null &&
                    sharedSpMgr.hasNodeActiveLocks(connPeer.getNode())
            )
            {
                taskScheduler.addTask(
                    new ForceReleaseSharedLocksTask(
                        connPeer.getNode(),
                        sharedSpMgr,
                        nodeInternalCallHandler
                    )
                );
            }
        }
    }
}
