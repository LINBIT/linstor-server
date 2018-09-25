package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
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
import java.util.stream.Stream;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;
import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlNodeDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlNodeDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        peerAccCtx = peerAccCtxRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (rsc.getAssignedNode().getFlags().isSet(apiCtx, Node.NodeFlag.DELETE))
            {
                NodeName nodeName = rsc.getAssignedNode().getName();
                fluxes.add(updateSatellites(nodeName, rscDfn.getName()));
            }
        }

        return fluxes;
    }

    /**
     * Deletes the given {@link Node} for deletion.
     *
     * The node is only deleted once the satellite confirms that it has no more
     * {@link Resource}s and {@link StorPool}s deployed.
     */
    public Flux<ApiCallRc> deleteNode(String nodeNameStr)
    {
        ResponseContext context = CtrlNodeApiCallHandler.makeNodeContext(
            ApiOperation.makeDeleteOperation(),
            nodeNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.readLock()),
                () -> deleteNodeInTransaction(context, nodeNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deleteNodeInTransaction(ResponseContext context, String nodeNameStr)
    {
        Flux<ApiCallRc> responseFlux;
        ApiCallRcImpl responses = new ApiCallRcImpl();

        requireNodesMapChangeAccess();
        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        NodeData node = ctrlApiDataLoader.loadNode(nodeName, false);
        if (node == null)
        {
            responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.WARN_NOT_FOUND,
                    "Deletion of node '" + nodeName + "' had no effect."
                )
                .setCause("Node '" + nodeName + "' does not exist.")
                .build()
            );
            responseFlux = Flux.just(responses);
        }
        else
        {
            // store to avoid deleted node acess
            UUID nodeUuid = node.getUuid();
            String nodeDescription = firstLetterCaps(getNodeDescriptionInline(node));

            markDeleted(node);
            for (Resource rsc : getRscStream(node).collect(toList()))
            {
                markDeleted(rsc);
            }

            boolean nodeDeleted = deleteNodeIfEmpty(node);

            ctrlTransactionHelper.commit();

            if (nodeDeleted)
            {
                Peer nodePeer = getPeerPriveleged(node);

                ApiCallRcImpl.ApiCallRcEntry response = disconnectNode(nodeUuid, nodeDescription, nodePeer);
                responseConverter.addWithOp(responses, context, response);

                responseFlux = Flux.just(responses);
            }
            else
            {
                responseConverter.addWithOp(responses, context, ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, nodeDescription + " marked for deletion.")
                    .setDetails(nodeDescription + " UUID is: " + nodeUuid.toString())
                    .build()
                );

                Collection<Node> affectedNodes = CtrlSatelliteUpdater.findNodesToContact(apiCtx, node);
                for (Node affectedNode: affectedNodes)
                {
                    if (getPeerPriveleged(affectedNode).getConnectionStatus() != Peer.ConnectionStatus.ONLINE)
                    {
                        responses.addEntry(ResponseUtils.makeNotConnectedWarning(affectedNode.getName()));
                    }
                }

                List<Flux<ApiCallRc>> resourceDeletionResponses = getRscStreamPriveleged(node)
                    .map(rsc -> updateSatellites(nodeName, rsc.getDefinition().getName()))
                    .collect(toList());

                responseFlux = Flux
                    .<ApiCallRc>just(responses)
                    .concatWith(Flux.merge(resourceDeletionResponses));
            }
        }

        return responseFlux;
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> updateSatellites(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> updateSatellitesInScope(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(NodeName nodeName, ResourceName rscName)
    {
        Flux<ApiCallRc> responseFlux;

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        if (rsc == null)
        {
            responseFlux = Flux.empty();
        }
        else
        {
            responseFlux = ctrlSatelliteUpdateCaller.updateSatellites(rsc)
                .transform(updateResponses -> ResponseUtils.translateDeploymentSuccess(
                    updateResponses,
                    nodeName,
                    "Deleted resource {1} on {0}",
                    "Notified {0} that resource {1} on ''" + nodeName + "'' is being deleted"
                ))
                .concatWith(resourceDeleted(nodeName, rscName))
                .onErrorResume(CtrlSatelliteUpdateCaller.DelayedApiRcException.class, ignored -> Flux.empty())
                // Suppress offline warnings because they were already generated in the first step
                .map(this::removeOfflineWarnings);
        }

        return responseFlux;
    }

    private ApiCallRc removeOfflineWarnings(ApiCallRc apiCallRc)
    {
        return new ApiCallRcImpl(
            apiCallRc.getEntries().stream()
                .filter(rcEntry -> rcEntry.getReturnCode() != ApiConsts.WARN_NOT_CONNECTED)
                .collect(toList())
        );
    }

    private Flux<ApiCallRc> resourceDeleted(NodeName nodeName, ResourceName rscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> resourceDeletedInTransaction(nodeName, rscName)
            );
    }

    private Flux<ApiCallRc> resourceDeletedInTransaction(
        NodeName nodeName,
        ResourceName rscName
    )
    {
        Flux<ApiCallRc> responseFlux;

        ResourceData rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        if (rsc == null)
        {
            responseFlux = Flux.empty();
        }
        else
        {
            Node node = rsc.getAssignedNode();
            // store to avoid deleted node acess
            UUID nodeUuid = node.getUuid();
            String nodeDescription = firstLetterCaps(getNodeDescriptionInline(node));

            deletePriveleged(rsc);

            boolean nodeDeleted = deleteNodeIfEmpty(node);
            ctrlTransactionHelper.commit();

            if (nodeDeleted)
            {
                Peer nodePeer = getPeerPriveleged(node);

                ApiCallRcImpl.ApiCallRcEntry response = disconnectNode(nodeUuid, nodeDescription, nodePeer);

                responseFlux = Flux.just(ApiCallRcImpl.singletonApiCallRc(response));
            }
            else
            {
                responseFlux = Flux.empty();
            }
        }

        return responseFlux;
    }

    private ApiCallRcImpl.ApiCallRcEntry disconnectNode(UUID nodeUuid, String nodeDescription, Peer nodePeer)
    {
        ApiCallRcImpl.ApiCallRcEntry response = ApiCallRcImpl
            .entryBuilder(ApiConsts.DELETED, nodeDescription + " deleted.")
            .setDetails(nodeDescription + " UUID was: " + nodeUuid.toString())
            .build();

        if (nodePeer != null)
        {
            nodePeer.closeConnection();
        }
        return response;
    }

    private boolean deleteNodeIfEmpty(Node node)
    {
        boolean canDelete = node.getResourceCount() == 0;
        if (canDelete)
        {

            // If the node has no resources, then there should not be any volumes referenced
            // by the storage pool -- double check
            Iterator<StorPool> storPoolIterator = getStorPoolIteratorPriveleged(node);
            while (storPoolIterator.hasNext())
            {
                StorPool storPool = storPoolIterator.next();
                if (!hasVolumesPriveleged(storPool))
                {
                    deletePriveleged(storPool);
                }
                else
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_VLM,
                        String.format(
                            "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                                "on this node, although the node does not reference any resources",
                            node.getName(),
                            storPool.getName()
                        )
                    ));
                }
            }

            NodeName nodeName = node.getName();
            deletePriveleged(node);
            removeNodePriveleged(nodeName);
        }
        return canDelete;
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

    private Peer getPeerPriveleged(Node node)
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

    private boolean hasVolumesPriveleged(StorPool storPool)
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

    private Stream<Resource> getRscStreamPriveleged(Node node)
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

    private Iterator<StorPool> getStorPoolIteratorPriveleged(Node node)
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

    private Stream<Resource> getRscStream(Node node)
    {
        Stream<Resource> stream;
        try
        {
            stream = node.streamResources(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get the resources of node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return stream;
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

    private void markDeleted(Resource rsc)
    {
        try
        {
            rsc.markDeleted(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete the resource " + rsc,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void deletePriveleged(Node node)
    {
        try
        {
            node.delete(apiCtx);
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

    private void deletePriveleged(StorPool storPool)
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

    private void deletePriveleged(Resource rsc)
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

    private void removeNodePriveleged(NodeName nodeName)
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
