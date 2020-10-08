package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SharedStorPoolManager;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import static java.util.stream.Collectors.toList;

public class NodeInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;
    private final ReadWriteLock nodesMapLock;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SharedStorPoolManager sharedStorPoolManager;

    @Inject
    public NodeInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SharedStorPoolManager sharedStorPoolManagerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        nodesMapLock = nodesMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        sharedStorPoolManager = sharedStorPoolManagerRef;
    }

    public void handleNodeRequest(UUID nodeUuid, String nodeNameStr)
    {
        try (LockGuard ls = LockGuard.createLocked(
            nodesMapLock.readLock(),
            peer.get().getSerializerLock().readLock()
        ))
        {
            Peer currentPeer = peer.get();
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = ctrlApiDataLoader.loadNode(nodeName, false);
            if (node != null && !node.isDeleted() && node.getFlags().isUnset(apiCtx, Node.Flags.DELETE))
            {
                if (node.getUuid().equals(nodeUuid))
                {
                    Collection<Node> otherNodes = new TreeSet<>();
                    // otherNodes can be filled with all nodes (except the current 'node')
                    // related to the satellite. The serializer only needs the other nodes for
                    // the nodeConnections.
                    for (Resource rsc : currentPeer.getNode().streamResources(apiCtx).collect(toList()))
                    {
                        Iterator<Resource> otherRscIterator = rsc.getDefinition().iterateResource(apiCtx);
                        while (otherRscIterator.hasNext())
                        {
                            Resource otherRsc = otherRscIterator.next();
                            if (otherRsc != rsc)
                            {
                                otherNodes.add(otherRsc.getNode());
                            }
                        }
                    }
                    long fullSyncTimestamp = currentPeer.getFullSyncId();
                    long serializerId = currentPeer.getNextSerializerId();
                    currentPeer.sendMessage(
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_APPLY_NODE)
                            .node(node, otherNodes, fullSyncTimestamp, serializerId)
                            .build()
                    );
                }
                else
                {
                    errorReporter.reportError(
                        new ImplementationError(
                            currentPeer + " requested a node with an outdated " +
                                "UUID. Current UUID: " + node.getUuid() + ", satellites outdated UUID: " +
                                nodeUuid,
                            null
                        )
                    );
                }
            }
            else
            {
                long fullSyncTimestamp = currentPeer.getFullSyncId();
                long serializerId = currentPeer.getNextSerializerId();
                currentPeer.sendMessage(
                    ctrlStltSerializer.onewayBuilder(InternalApiConsts.API_APPLY_NODE_DELETED)
                        .deletedNode(nodeNameStr, fullSyncTimestamp, serializerId)
                        .build()
                );
            }
        }
        catch (Exception exc)
        {
            errorReporter.reportError(
                new ImplementationError(exc)
            );
        }
    }

    public void handleSharedStorPoolLockRequest(List<String> sharedStorPoolLocksListRef)
    {
        Peer currentPeer = peer.get();
        Node node = currentPeer.getNode();

        // node is null if the peer calling this API was not a satellite...
        if (node != null)
        {
            try
            {
                Set<SharedStorPoolName> locks = new TreeSet<>();
                for (String sharedLockStr : sharedStorPoolLocksListRef)
                {
                    locks.add(new SharedStorPoolName(sharedLockStr));
                }
                boolean acquired = sharedStorPoolManager.requestSharedLocks(node, locks);
                if (acquired)
                {
                    updateStlt(node, locks);
                }
            }
            catch (InvalidNameException | AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private void updateStlt(Node node, Set<SharedStorPoolName> locks) throws AccessDeniedException
    {
        Peer peer = node.getPeer(apiCtx);
        peer.sendMessage(
            ctrlStltSerializer.onewayBuilder(InternalApiConsts.API_APPLY_SHARED_STOR_POOL_LOCKS)
                .grantsharedStorPoolLocks(locks)
                .build()
        );
    }

    public void handleDevMgrRunCompleted()
    {
        Node node = peer.get().getNode();

        // node is null if the peer calling this API was not a satellite...
        if (node != null)
        {
            errorReporter.logTrace("%s finished with devMgr. Releasing locks", node);

            Map<Node, Set<SharedStorPoolName>> nodesToUpdate = sharedStorPoolManager.releaseLocks(node);

            try
            {
                for (Entry<Node, Set<SharedStorPoolName>> entry : nodesToUpdate.entrySet())
                {
                    updateStlt(entry.getKey(), entry.getValue());
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }
}
