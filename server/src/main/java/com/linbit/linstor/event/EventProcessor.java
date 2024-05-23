package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.linstor.transaction.manager.TransactionMgrGenerator;
import com.linbit.linstor.transaction.manager.TransactionMgrUtil;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.google.inject.Key;
import reactor.core.publisher.Flux;

/**
 * Processes incoming events
 */
@Singleton
public class EventProcessor
{
    private final ErrorReporter errorReporter;
    private final Map<String, Provider<EventHandler>> eventHandlers;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<Peer> peerProvider;
    private final AccessContext sysCtx;
    private final LinStorScope linstorScope;

    // Synchronizes access to incomingEventStreamStore and pendingEventsPerPeer
    private final ReentrantLock eventHandlingLock;

    private final EventStreamStore incomingEventStreamStore;
    private final Provider<TransactionMgr> transMgrProvider;
    private final TransactionMgrGenerator transactionMgrGenerator;

    @Inject
    public EventProcessor(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        Map<String, Provider<EventHandler>> eventHandlersRef,
        LockGuardFactory lockGuardFactoryRef,
        Provider<Peer> peerProviderRef,
        LinStorScope linstorScopeRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionMgrGenerator transactionMgrGeneratorRef
    )
    {
        errorReporter = errorReporterRef;
        eventHandlers = eventHandlersRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerProvider = peerProviderRef;
        linstorScope = linstorScopeRef;
        sysCtx = sysCtxRef;
        transMgrProvider = transMgrProviderRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;

        eventHandlingLock = new ReentrantLock();
        incomingEventStreamStore = new EventStreamStoreImpl();
    }

    public void connectionClosed(Peer peer)
    {
        ArrayList<NodeName> nodeNamesToProcess = new ArrayList<>();
        try (LockGuard lockGuard = lockGuardFactory.build(LockGuardFactory.LockType.READ, NODES_MAP))
        {
            Node node = peer.getNode();
            if (node != null && !node.isDeleted())
            {
                nodeNamesToProcess.add(node.getName());
                // we MUST NOT call "executeNoConnection" method from here, since that method might start a
                // database-transaction.
                // we MUST NOT take linstor-locks (which we currently already have) before starting a
                // database-transaction, otherwise we might run into deadlocks with database internal locks
            }
        }
        if (!nodeNamesToProcess.isEmpty())
        {
            eventHandlingLock.lock();
            try
            {
                for (NodeName nodeName : nodeNamesToProcess)
                {
                    // The peer is a Satellite
                    for (Map.Entry<String, Provider<EventHandler>> eventHandlerEntry : eventHandlers.entrySet())
                    {
                        Collection<EventIdentifier> eventStreams = incomingEventStreamStore.getDescendantEventStreams(
                            EventIdentifier.node(eventHandlerEntry.getKey(), nodeName)
                        );

                        for (EventIdentifier eventIdentifier : eventStreams)
                        {
                            executeNoConnection(eventHandlerEntry.getValue(), eventIdentifier);

                            incomingEventStreamStore.removeEventStream(eventIdentifier);
                        }
                    }
                }
            }
            finally
            {
                eventHandlingLock.unlock();
            }
        }
    }

    public Flux<?> handleEvent(
        String eventAction,
        String eventName,
        String resourceNameStr,
        Integer volumeNr,
        String snapshotNameStr,
        String peerNodeNameStr,
        InputStream eventDataIn
    )
    {
        try (LockGuard lockGuard = lockGuardFactory.build(LockGuardFactory.LockType.WRITE, NODES_MAP))
        {
            eventHandlingLock.lock();
            try
            {
                Provider<EventHandler> eventHandlerProvider = eventHandlers.get(eventName);
                if (eventHandlerProvider == null)
                {
                    errorReporter.logWarning("Unknown event '%s' received", eventName);
                }
                else
                {
                    ResourceName resourceName =
                        resourceNameStr != null ? new ResourceName(resourceNameStr) : null;
                    VolumeNumber volumeNumber =
                        volumeNr != null ? new VolumeNumber(volumeNr) : null;
                    SnapshotName snapshotName =
                        snapshotNameStr != null ? new SnapshotName(snapshotNameStr) : null;
                    NodeName peerNodeName = peerNodeNameStr != null ? new NodeName(peerNodeNameStr) : null;

                    Peer peer = peerProvider.get();
                    EventIdentifier eventIdentifier = new EventIdentifier(
                        eventName,
                        new ObjectIdentifier(
                            peer.getNode().getName(),
                            resourceName,
                            volumeNumber,
                            snapshotName,
                            peerNodeName
                        )
                    );

                    incomingEventStreamStore.addEventStreamIfNew(eventIdentifier);

                    errorReporter.logTrace("Peer %s, event '%s' %s", peer, eventIdentifier, eventAction);
                    eventHandlerProvider.get().execute(eventAction, eventIdentifier, eventDataIn);

                    if (eventAction.equals(InternalApiConsts.EVENT_STREAM_CLOSE_REMOVED))
                    {
                        incomingEventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
                commit(transMgrProvider.get());
            }
            catch (InvalidNameException | ValueOutOfRangeException exc)
            {
                errorReporter.logWarning("Invalid event received: " + exc.getMessage());
            }
            catch (Exception | ImplementationError exc)
            {
                errorReporter.reportError(exc);
            }
            finally
            {
                eventHandlingLock.unlock();
            }
        }
        return Flux.empty();
    }

    private void executeNoConnection(
        Provider<EventHandler> eventHandler,
        EventIdentifier eventIdentifier
    )
    {
        TransactionMgr transMgr = null;
        boolean inScope = linstorScope.isEntered();
        boolean seedTxMgr = false;
        try (LinStorScope.ScopeAutoCloseable close = inScope ? null : linstorScope.enter())
        {
            if (!inScope)
            {
                linstorScope.seed(Key.get(AccessContext.class, PeerContext.class), sysCtx);
                seedTxMgr = true;
            }
            else
            {
                if (linstorScope.isSeeded(Key.get(TransactionMgr.class)))
                {
                    transMgr = transMgrProvider.get();
                }
                else
                {
                    seedTxMgr = true;
                }
            }
            if (seedTxMgr)
            {
                transMgr = transactionMgrGenerator.startTransaction();
                TransactionMgrUtil.seedTransactionMgr(linstorScope, transMgr);
            }
            try (LockGuard lockGuard = lockGuardFactory.build(LockGuardFactory.LockType.READ, NODES_MAP))
            {
                eventHandler.get()
                    .execute(
                        InternalApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION,
                        eventIdentifier,
                        null
                    );

                commit(transMgr);
            }
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc, null, null,
                "Event handler for " + eventIdentifier + " failed on connection closed");
        }
        finally
        {
            if (seedTxMgr) // only return/close connection of transMgr if we created it
            {
                rollbackIfNeededAndCloseConn(transMgr);
            }
        }
    }

    private void commit(TransactionMgr transMgr)
    {
        if (transMgr != null && transMgr.isDirty())
        {
            try
            {
                transMgr.commit();
            }
            catch (TransactionException sqlExc)
            {
                errorReporter.reportError(sqlExc);
                rollbackIfNeededAndCloseConn(transMgr);
            }
        }
    }

    private void rollbackIfNeededAndCloseConn(TransactionMgr transMgr)
    {
        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (TransactionException exc)
                {
                    errorReporter.reportError(exc);
                    // TODO: needs a better panic-shutdown to prevent DB corruption
                    System.exit(1);
                }
            }
            transMgr.returnConnection();
        }
    }
}
