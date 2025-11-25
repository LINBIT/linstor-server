package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SharedStorPoolManager;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.common.base.Objects;

import static java.util.stream.Collectors.toList;

public class NodeInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peerProvider;
    private final ReadWriteLock nodesMapLock;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SharedStorPoolManager sharedStorPoolManager;
    private final CtrlSatelliteUpdater stltUpdater;
    private final CtrlTransactionHelper ctrlTransactionHelper;

    @Inject
    public NodeInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SharedStorPoolManager sharedStorPoolManagerRef,
        CtrlSatelliteUpdater stltUpdaterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peerProvider = peerRef;
        nodesMapLock = nodesMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        sharedStorPoolManager = sharedStorPoolManagerRef;
        stltUpdater = stltUpdaterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
    }

    public void handleNodeRequest(UUID nodeUuid, String nodeNameStr)
    {
        try (LockGuard ls = LockGuard.createLocked(
            nodesMapLock.readLock(),
            peerProvider.get().getSerializerLock().readLock()
        ))
        {
            Peer currentPeer = peerProvider.get();
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
                        Iterator<Resource> otherRscIterator = rsc.getResourceDefinition().iterateResource(apiCtx);
                        while (otherRscIterator.hasNext())
                        {
                            Resource otherRsc = otherRscIterator.next();
                            if (otherRsc != rsc)
                            {
                                otherNodes.add(otherRsc.getNode());
                            }
                        }
                    }

                    for (NodeConnection nodeConn : node.getNodeConnections(apiCtx))
                    {
                        otherNodes.add(nodeConn.getOtherNode(apiCtx, node));
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
        Peer currentPeer = peerProvider.get();
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
        Peer stltPeer = node.getPeer(apiCtx);
        stltPeer.sendMessage(
            ctrlStltSerializer.onewayBuilder(InternalApiConsts.API_APPLY_SHARED_STOR_POOL_LOCKS)
                .grantsharedStorPoolLocks(locks)
                .build()
        );
    }

    public void handleDevMgrRunCompleted()
    {
        Node node = peerProvider.get().getNode();

        // node is null if the peer calling this API was not a satellite...
        if (node != null)
        {
            errorReporter.logTrace("%s finished with devMgr. Releasing locks", node);

            releaseLocks(node);
        }
    }

    public void releaseLocks(Node node)
    {
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

    public void handleNodeUpdate(
        Map<String, String> changedPropsRef,
        List<String> deletedPropsListRef,
        Map<String, Map<String, String>> changedStorPoolPropsRef,
        Map<String, List<String>> deletedStorPoolPropsRef
    )
    {
        Peer peer = peerProvider.get();
        @Nullable Node node = peer.getNode();

        if (node != null && !node.isDeleted())
        {
            try (
                LockGuard ls = LockGuard.createLocked(
                    nodesMapLock.writeLock(),
                    peer.getSerializerLock().readLock()
                )
            )
            {
                Props props = node.getProps(apiCtx);
                boolean changedNode = false;
                changedNode |= delete(props, deletedPropsListRef);
                changedNode |= update(props, changedPropsRef);

                Set<StorPool> changedStorPoolSet = new HashSet<>();
                for (Entry<String, List<String>> entry : deletedStorPoolPropsRef.entrySet())
                {
                    StorPool storPool = ctrlApiDataLoader.loadStorPool(entry.getKey(), node, true);
                    Props spProps = storPool.getProps(apiCtx);
                    boolean changedSp = delete(spProps, entry.getValue());
                    if (changedSp)
                    {
                        changedStorPoolSet.add(storPool);
                    }
                }
                for (Entry<String, Map<String, String>> entry : changedStorPoolPropsRef.entrySet())
                {
                    StorPool storPool = ctrlApiDataLoader.loadStorPool(entry.getKey(), node, true);
                    Props spProps = storPool.getProps(apiCtx);
                    boolean changedSp = update(spProps, entry.getValue());
                    if (changedSp)
                    {
                        changedStorPoolSet.add(storPool);
                    }
                }

                if (changedNode || !changedStorPoolSet.isEmpty())
                {
                    ctrlTransactionHelper.commit();

                    if (changedNode)
                    {
                        stltUpdater.updateSatellites(node);
                    }
                    for (StorPool storPool : changedStorPoolSet)
                    {
                        stltUpdater.updateSatellite(storPool);
                    }
                }
            }
            catch (AccessDeniedException | InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                errorReporter.reportError(exc);
            }
        }
        else
        {
            if (node == null)
            {
                errorReporter.logWarning("Ignored node update since peer %s has no node attached", peer.getId());
            }
            else
            {
                errorReporter.logWarning(
                    "Ignored node update since node '%s' is already deleted",
                    node.getKey().displayValue
                );
            }
        }
    }

    private boolean delete(Props propsRef, List<String> deletedPropsListRef)
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        boolean changed = false;
        for (String key : deletedPropsListRef)
        {
            changed |= propsRef.removeProp(key) != null;
        }
        return changed;
    }

    private boolean update(Props propsRef, Map<String, String> changedPropsRef)
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        boolean changed = false;
        for (Entry<String, String> entry : changedPropsRef.entrySet())
        {
            String value = entry.getValue();
            changed |= !Objects.equal(value, propsRef.setProp(entry.getKey(), value));
        }
        return changed;
    }
}
