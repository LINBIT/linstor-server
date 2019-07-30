package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeRepository;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import static java.util.stream.Collectors.toList;

public class NodeInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final NodeRepository nodeRepository;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;

    private final ReadWriteLock nodesMapLock;

    @Inject
    public NodeInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        NodeRepository nodeRepositoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        nodeRepository = nodeRepositoryRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        nodesMapLock = nodesMapLockRef;
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

            Node node = nodeRepository.get(apiCtx, nodeName); // TODO use CtrlApiLoader.loadNode
            if (node != null && !node.isDeleted() && node.getFlags().isUnset(apiCtx, Node.NodeFlag.DELETE))
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
                                otherNodes.add(otherRsc.getAssignedNode());
                            }
                        }
                    }
                    long fullSyncTimestamp = currentPeer.getFullSyncId();
                    long serializerId = currentPeer.getNextSerializerId();
                    currentPeer.sendMessage(
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_APPLY_NODE)
                            .nodeData(node, otherNodes, fullSyncTimestamp, serializerId)
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
                        .deletedNodeData(nodeNameStr, fullSyncTimestamp, serializerId)
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
}
