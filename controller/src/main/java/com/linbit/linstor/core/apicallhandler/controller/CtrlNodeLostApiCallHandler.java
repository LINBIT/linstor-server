package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SwordfishTargetProcessManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;
import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlNodeLostApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifier;
    private final ReconnectorTask reconnectorTask;
    private final SwordfishTargetProcessManager sfTargetProcessMgr;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlNodeLostApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifierRef,
        ReconnectorTask reconnectorTaskRef, SwordfishTargetProcessManager sfTargetProcessMgrRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        reconnectorTask = reconnectorTaskRef;
        sfTargetProcessMgr = sfTargetProcessMgrRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    /**
     * Deletes an unrecoverable {@link Node}.
     *
     * This call is used if a node can't be reached or recovered, but has to be removed from the system.
     */
    public Flux<ApiCallRc> lostNode(String nodeNameStr)
    {
        ResponseContext context = CtrlNodeApiCallHandler.makeNodeContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Remove lost node",
                LockGuard.createDeferred(nodesMapLock.writeLock()),
                () -> lostNodeInTransaction(nodeNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> lostNodeInTransaction(String nodeNameStr)
    {
        requireNodesMapChangeAccess();
        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        NodeData nodeData = ctrlApiDataLoader.loadNode(nodeName, false);
        if (nodeData == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.WARN_NOT_FOUND,
                    "Deletion of node '" + nodeName + "' had no effect."
                )
                .setCause("Node '" + nodeName + "' does not exist.")
                .build()
            );
        }

        Peer nodePeer = getPeerPrivileged(nodeData);
        if (nodePeer != null && nodePeer.isConnected())
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_NODE_CONN,
                String.format(
                    "Node '%s' still connected, please use the '%s' api.",
                    nodeNameStr,
                    ApiConsts.API_DEL_NODE
                )
            ));
        }

        // find nodes that will need to be contacted before deleting the resources
        Map<NodeName, Node> nodesToContact = new TreeMap<>();

        // compile a list of used resource definitions that should be checked if they are now fully connected
        List<ResourceDefinition> rscDfnToCheck = new ArrayList<>();

        for (Resource rsc : getRscStreamPrivileged(nodeData).collect(toList()))
        {
            ResourceDefinition rscDfn = rsc.getDefinition();
            rscDfnToCheck.add(rscDfn);

            if (!isMarkedForDeletion(rsc))
            {
                for (Resource peerRsc : getRscStreamPrivileged(rscDfn).collect(toList()))
                {
                    nodesToContact.put(peerRsc.getAssignedNode().getName(), peerRsc.getAssignedNode());
                }
            }
            deletePrivileged(rsc);
        }
        // make sure that the just "lost" node is not contacted
        nodesToContact.remove(nodeName);

        // set node mark deleted for updates to other satellites
        markDeleted(nodeData);

        // If the node has no resources, then there should not be any volumes referenced
        // by the storage pool -- double check and delete storage pools
        Iterator<StorPool> storPoolIterator = getStorPoolIteratorPrivileged(nodeData);
        while (storPoolIterator.hasNext())
        {
            StorPool storPool = storPoolIterator.next();
            if (!hasVolumesPrivileged(storPool))
            {
                deletePrivileged(storPool);
            }
            else
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_VLM,
                    String.format(
                        "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                            "on this node, although the node does not reference any resources",
                        nodeNameStr,
                        storPool.getName().displayValue
                    )
                ));
            }
        }

        Collection<Snapshot> snapshots = new ArrayList<>(getSnapshotsPrivileged(nodeData));
        for (Snapshot snapshot : snapshots)
        {
            deletePrivileged(snapshot);
        }

        String successMessage = firstLetterCaps(getNodeDescriptionInline(nodeNameStr)) + " deleted.";
        UUID nodeUuid = nodeData.getUuid(); // store node uuid to avoid deleted node access

        deletePrivileged(nodeData);

        removeNodePrivileged(nodeName);

        ctrlTransactionHelper.commit();

        reconnectorTask.removePeer(nodePeer);

        // It may be possible to continue some operations since we are no longer waiting for the node to come online
        Flux<?> operationContinuation = Flux.merge(
            rscDfnToCheck.stream()
                .map(ctrlSatelliteConnectionNotifier::checkResourceDefinitionConnected)
                .collect(Collectors.toSet())
        );

        ApiCallRc responses = ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
            .entryBuilder(ApiConsts.DELETED, successMessage)
            .setDetails(firstLetterCaps(getNodeDescriptionInline(nodeNameStr)) +
                " UUID was: " + nodeUuid.toString())
            .build()
        );

        // inform other satellites that the node is gone
        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(nodeUuid, nodeName, nodesToContact.values())
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    null,
                    "Notified {0} that ''" + nodeName + "'' has been lost"
                ))
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());

        return Flux
            .just(responses)
            .concatWith(satelliteUpdateResponses)
            .concatWith(operationContinuation.thenMany(Flux.empty()));
    }

    private void requireNodesMapChangeAccess()
    {
        try
        {
            nodeRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any nodes",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
    }

    private Peer getPeerPrivileged(Node node)
    {
        Peer nodePeer;
        try
        {
            nodePeer = node.getPeer(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return nodePeer;
    }

    private boolean hasVolumesPrivileged(StorPool storPool)
    {
        boolean hasVolumes;
        try
        {
            hasVolumes = !storPool.getVolumes(apiCtx).isEmpty();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return hasVolumes;
    }

    private Stream<Resource> getRscStreamPrivileged(Node node)
    {
        Stream<Resource> stream;
        try
        {
            stream = node.streamResources(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return stream;
    }

    private Stream<Resource> getRscStreamPrivileged(ResourceDefinition rscDfn)
    {
        Stream<Resource> stream;
        try
        {
            stream = rscDfn.streamResource(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return stream;
    }

    private Iterator<StorPool> getStorPoolIteratorPrivileged(Node node)
    {
        Iterator<StorPool> iterateStorPools;
        try
        {
            // Shallow-copy the storage pool map, because the Iterator is used for
            // Node.delete(), which removes objects from the original map
            Map<StorPoolName, StorPool> storPoolMap = new TreeMap<>();
            node.copyStorPoolMap(apiCtx, storPoolMap);

            iterateStorPools = storPoolMap.values().iterator();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterateStorPools;
    }

    private Collection<Snapshot> getSnapshotsPrivileged(NodeData nodeData)
    {
        Collection<Snapshot> snapshots;
        try
        {
            snapshots = nodeData.getSnapshots(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return snapshots;
    }

    private boolean isMarkedForDeletion(Resource rsc)
    {
        boolean isMarkedForDeletion;
        try
        {
            isMarkedForDeletion = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.RscFlags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check deleted status of " + rsc,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return isMarkedForDeletion;
    }

    private void markDeleted(NodeData node)
    {
        try
        {
            node.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete the node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePrivileged(Node node)
    {
        try
        {
            Node.NodeType nodeType = node.getNodeType(apiCtx);
            node.delete(peerAccCtx.get());

            if (Node.NodeType.SWORDFISH_TARGET.equals(nodeType))
            {
                sfTargetProcessMgr.stopProcess(node);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete the node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePrivileged(StorPool storPool)
    {
        try
        {
            storPool.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePrivileged(Resource rsc)
    {
        try
        {
            rsc.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePrivileged(Snapshot snapshot)
    {
        try
        {
            snapshot.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void removeNodePrivileged(NodeName nodeName)
    {
        try
        {
            nodeRepository.remove(apiCtx, nodeName);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }
}
