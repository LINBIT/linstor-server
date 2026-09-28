package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
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
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
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

@Singleton
public class NodeInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peerProvider;
    private final ReadWriteLock nodesMapLock;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SharedStorPoolManager sharedStorPoolManager;
    private final CtrlSatelliteUpdater stltUpdater;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    public NodeInternalCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SharedStorPoolManager sharedStorPoolManagerRef,
        CtrlSatelliteUpdater stltUpdaterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peerProvider = peerRef;
        nodesMapLock = nodesMapLockRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        sharedStorPoolManager = sharedStorPoolManagerRef;
        stltUpdater = stltUpdaterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        lockGuardFactory = lockGuardFactoryRef;
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

            @Nullable Node node = ctrlApiDataLoader.loadNodeOrNull(nodeName);
            if (node != null && !node.isDeleted() && node.getFlags().isUnset(Node.Flags.DELETE))
            {
                if (node.getUuid().equals(nodeUuid))
                {
                    Collection<Node> otherNodes = new TreeSet<>();
                    // otherNodes can be filled with all nodes (except the current 'node')
                    // related to the satellite. The serializer only needs the other nodes for
                    // the nodeConnections.
                    for (Resource rsc : currentPeer.getNode().streamResources().collect(toList()))
                    {
                        Iterator<Resource> otherRscIterator = rsc.getResourceDefinition().iterateResource();
                        while (otherRscIterator.hasNext())
                        {
                            Resource otherRsc = otherRscIterator.next();
                            if (!otherRsc.equals(rsc))
                            {
                                otherNodes.add(otherRsc.getNode());
                            }
                        }
                    }

                    for (NodeConnection nodeConn : node.getNodeConnections())
                    {
                        otherNodes.add(nodeConn.getOtherNode(node));
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
        if (node != null && !node.isDeleted())
        {
            try (
                LockGuard lg = lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .postLinstorLocks(currentPeer.getSerializerLock().readLock())
                    .build())
            {
                if (!node.isDeleted())
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
            }
            catch (InvalidNameException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private void updateStlt(Node node, Set<SharedStorPoolName> locks)
    {
        node.getPeer()
            .sendMessage(
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
            // it is possible that after the updateSatellites that included the DELETE flag of the satellite itself
            // we also sent another updateSatellite asynchronously. In that case the satellite will happily process
            // the DELETE request, the controller deletes the node from the database and sets the node's internal
            // delete boolean and yet the satellite still sends us another response (to the later async request) that
            // also lands here, but at a time the node is fully deleted already. The Peer object might still refer
            // to it though.
            // Therefore we are using .getKey() instead of .getName() here since it does not perform a checkDeleted.
            // We must release the locks even if the node got deleted in the meantime.
            NodeName nodeName = node.getKey();
            errorReporter.logTrace("%s finished with devMgr. Releasing locks", nodeName);

            releaseLocks(nodeName);
        }
    }

    public void releaseLocks(NodeName nodeNameRef)
    {
        Map<NodeName, Set<SharedStorPoolName>> nodesToUpdate = sharedStorPoolManager.releaseLocks(nodeNameRef);

        if (!nodesToUpdate.isEmpty())
        {
            try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.NODES_MAP))
            {
                Deque<Map<NodeName, Set<SharedStorPoolName>>> todoList = new ArrayDeque<>();
                todoList.add(nodesToUpdate);
                while (!todoList.isEmpty())
                {
                    Map<NodeName, Set<SharedStorPoolName>> map = todoList.removeFirst();
                    for (Entry<NodeName, Set<SharedStorPoolName>> entry : map.entrySet())
                    {
                        NodeName nextNodeName = entry.getKey();
                        @Nullable Node node = ctrlApiDataLoader.loadNodeOrNull(nextNodeName, false);
                        if (node != null && !node.isDeleted())
                        {
                            updateStlt(node, entry.getValue());
                        }
                        else
                        {
                            // imagine the following case:
                            // * nodeNameRef refers to node "a", which was holding a lock
                            // * node "b" was also waiting for that lock, so it was queued
                            // * before node "a" released the lock, node "b" got deleted
                            // * this else case is exactly this case, with entry.getKey() == "b"
                            // this means that we need pretend node "b" has now also released the lock to continue
                            // with a possibly next waiting node
                            todoList.add(sharedStorPoolManager.releaseLocks(nextNodeName));
                        }
                    }
                }
            }
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
                // check again now that we have the lock
                if (!node.isDeleted())
                {
                    Props props = node.getProps();
                    boolean changedNode = false;
                    changedNode |= delete(props, deletedPropsListRef);
                    changedNode |= update(props, changedPropsRef);

                    Set<StorPool> changedStorPoolSet = new HashSet<>();
                    for (Entry<String, List<String>> entry : deletedStorPoolPropsRef.entrySet())
                    {
                        StorPool storPool = ctrlApiDataLoader.loadStorPool(entry.getKey(), node);
                        Props spProps = storPool.getProps();
                        boolean changedSp = delete(spProps, entry.getValue());
                        if (changedSp)
                        {
                            changedStorPoolSet.add(storPool);
                        }
                    }
                    for (Entry<String, Map<String, String>> entry : changedStorPoolPropsRef.entrySet())
                    {
                        StorPool storPool = ctrlApiDataLoader.loadStorPool(entry.getKey(), node);
                        Props spProps = storPool.getProps();
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
            }
            catch (InvalidKeyException | InvalidValueException exc)
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
        throws InvalidKeyException, DatabaseException
    {
        boolean changed = false;
        for (String key : deletedPropsListRef)
        {
            changed |= propsRef.removeProp(key) != null;
        }
        return changed;
    }

    private boolean update(Props propsRef, Map<String, String> changedPropsRef)
        throws InvalidKeyException, DatabaseException, InvalidValueException
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
