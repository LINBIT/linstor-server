package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlNodeDeleteApiCallHandler implements CtrlSatelliteConnectionListener
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final Provider<CtrlSatelliteConnectionNotifier> ctrlSatelliteConnectionNotifier;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandler;

    @Inject
    public CtrlNodeDeleteApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        Provider<CtrlSatelliteConnectionNotifier> ctrlSatelliteConnectionNotifierRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapshotDeleteApiCallHandlerRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        ctrlSnapshotDeleteApiCallHandler = ctrlSnapshotDeleteApiCallHandlerRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
        throws AccessDeniedException
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        Iterator<Resource> rscIter = rscDfn.iterateResource(apiCtx);
        while (rscIter.hasNext())
        {
            Resource rsc = rscIter.next();
            if (rsc.getNode().getFlags().isSet(apiCtx, Node.Flags.DELETE))
            {
                NodeName nodeName = rsc.getNode().getName();
                fluxes.add(updateSatellites(nodeName, rscDfn.getName(), context));
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
                "Delete node",
                lockGuardFactory.createDeferred()
                    .write(LockObj.NODES_MAP)
                    .read(LockObj.RSC_DFN_MAP)
                    .build(),
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
        Node node = ctrlApiDataLoader.loadNode(nodeName, false);
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
            // store to avoid deleted node access
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
                Peer nodePeer = getPeerPrivileged(node);

                responses.addEntry(disconnectNode(nodeUuid, nodeDescription, nodePeer));

                responseFlux = Flux.just(responses);
            }
            else
            {
                responses.addEntry(ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, nodeDescription + " marked for deletion.")
                    .setDetails(nodeDescription + " UUID is: " + nodeUuid.toString())
                    .build()
                );

                Collection<Node> affectedNodes = CtrlSatelliteUpdater.findNodesToContact(apiCtx, node);
                for (Node affectedNode: affectedNodes)
                {
                    if (getPeerPrivileged(affectedNode).getConnectionStatus() != Peer.ConnectionStatus.ONLINE)
                    {
                        responses.addEntry(ResponseUtils.makeNotConnectedWarning(affectedNode.getName()));
                    }
                }

                List<Flux<ApiCallRc>> resourceDeletionResponses = getRscStreamPrivileged(node)
                    .map(rsc -> updateSatellites(nodeName, rsc.getDefinition().getName(), context))
                    .collect(toList());

                responseFlux = Flux
                    .<ApiCallRc>just(responses)
                    .concatWith(Flux.merge(deleteSnapshotsPrivileged(node)))
                    .concatWith(Flux.merge(resourceDeletionResponses));
            }
        }

        return responseFlux;
    }

    // Restart from here when connection established and DELETE flag set
    private Flux<ApiCallRc> updateSatellites(NodeName nodeName, ResourceName rscName, ResponseContext context)
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Update for node deletion",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> updateSatellitesInScope(nodeName, rscName, context)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(NodeName nodeName, ResourceName rscName, ResponseContext context)
    {
        Flux<ApiCallRc> responseFlux;

        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        if (rsc == null)
        {
            responseFlux = Flux.empty();
        }
        else
        {
            Flux<ApiCallRc> nextStep = resourceDeleted(nodeName, rscName, context);
            responseFlux = ctrlSatelliteUpdateCaller.updateSatellites(rsc, nextStep)
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscName,
                    Collections.singleton(nodeName),
                    "Deleted resource {1} on {0}",
                    "Notified {0} that resource {1} on ''" + nodeName + "'' is being deleted"
                ))
                .concatWith(nextStep)
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty())
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

    private Flux<ApiCallRc> resourceDeleted(NodeName nodeName, ResourceName rscName, ResponseContext context)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resource deleted",
                lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> resourceDeletedInTransaction(nodeName, rscName, context)
            );
    }

    private Flux<ApiCallRc> resourceDeletedInTransaction(
        NodeName nodeName,
        ResourceName rscName,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> responseFlux;

        Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, rscName, false);

        if (rsc == null)
        {
            responseFlux = Flux.empty();
        }
        else
        {
            // store to avoid deleted data access
            Node node = rsc.getNode();
            UUID nodeUuid = node.getUuid();
            String nodeDescription = firstLetterCaps(getNodeDescriptionInline(node));
            ResourceDefinition rscDfn = rsc.getDefinition();

            deletePrivileged(rsc);

            boolean nodeDeleted = deleteNodeIfEmpty(node);
            ctrlTransactionHelper.commit();

            if (nodeDeleted)
            {
                Peer nodePeer = getPeerPrivileged(node);

                ApiCallRcImpl.ApiCallRcEntry response = disconnectNode(nodeUuid, nodeDescription, nodePeer);

                // Some operations may have been waiting for API calls to the satellite to complete.
                // They will receive notification that the node has been disconnected, but they may
                // need to react to the deletion of the resource and hence we need to check whether
                // they can continue. For instance, when a resource definition and node are being
                // deleted at the same time.
                Flux<?> operationContinuation =
                    ctrlSatelliteConnectionNotifier.get().checkResourceDefinitionConnected(rscDfn, context);

                responseFlux = Flux
                    .<ApiCallRc>just(ApiCallRcImpl.singletonApiCallRc(response))
                    .concatWith(operationContinuation.thenMany(Flux.<ApiCallRc>empty()));
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
            Iterator<StorPool> storPoolIterator = getStorPoolIteratorPrivileged(node);
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
                            node.getName(),
                            storPool.getName()
                        )
                    ));
                }
            }

            NodeName nodeName = node.getName();
            deletePrivileged(node);
            removeNodePrivileged(nodeName);
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

    private List<Flux<ApiCallRc>> deleteSnapshotsPrivileged(Node node)
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        try
        {
            for (Snapshot snapshot : node.getSnapshots(apiCtx))
            {
                fluxes.add(ctrlSnapshotDeleteApiCallHandler.deleteSnapshot(
                    snapshot.getResourceName().getName(), snapshot.getSnapshotName().getName()));
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get all snapshots of '" + node.getName().displayValue,
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }

        return fluxes;
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

    private void markDeleted(Node node)
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
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
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
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void deletePrivileged(Node node)
    {
        try
        {
            node.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
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
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
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
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
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
