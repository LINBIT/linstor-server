package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SpecialSatelliteProcessManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.backup.CtrlBackupCreateApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.numberpool.DynamicNumberPool;
import com.linbit.linstor.numberpool.NumberPoolModule;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler.getNodeDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

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

import reactor.core.publisher.Flux;

import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlNodeLostApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifier;
    private final ReconnectorTask reconnectorTask;
    private final SpecialSatelliteProcessManager specTargetProcMgr;
    private final DynamicNumberPool specStltPortPool;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeRepository nodeRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final ReadWriteLock nodesMapLock;
    private final CtrlRscDeleteApiHelper ctrlRscDeleteApiHelper;
    private final CtrlBackupCreateApiCallHandler ctrlBackupCrtApiCallHandler;

    @Inject
    public CtrlNodeLostApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlSatelliteConnectionNotifier ctrlSatelliteConnectionNotifierRef,
        ReconnectorTask reconnectorTaskRef,
        SpecialSatelliteProcessManager specStltTargetProcMgrRef,
        @Named(NumberPoolModule.SPECIAL_SATELLTE_PORT_POOL) DynamicNumberPool specStltPortPoolRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeRepository nodeRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CtrlRscDeleteApiHelper ctrlRscDeleteApiHelperRef,
        CtrlBackupCreateApiCallHandler ctrlBackupCrtApiCallHandlerRef,
        ErrorReporter errorReporterRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlSatelliteConnectionNotifier = ctrlSatelliteConnectionNotifierRef;
        reconnectorTask = reconnectorTaskRef;
        specTargetProcMgr = specStltTargetProcMgrRef;
        specStltPortPool = specStltPortPoolRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeRepository = nodeRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        nodesMapLock = nodesMapLockRef;
        ctrlRscDeleteApiHelper = ctrlRscDeleteApiHelperRef;
        ctrlBackupCrtApiCallHandler = ctrlBackupCrtApiCallHandlerRef;
        errorReporter = errorReporterRef;
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

        return ctrlBackupCrtApiCallHandler.deleteNodeQueueAndReQueueSnapsIfNeeded(nodeNameStr)
            .concatWith(
                scopeRunner
                    .fluxInTransactionalScope(
                        "Remove lost node",
                        LockGuard.createDeferred(nodesMapLock.writeLock()),
                        () -> lostNodeInTransaction(nodeNameStr, context)
                    )
                    .transform(responses -> responseConverter.reportingExceptions(context, responses))
            );
    }

    private Flux<ApiCallRc> lostNodeInTransaction(String nodeNameStr, ResponseContext context)
    {
        requireNodesMapChangeAccess();
        NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
        @Nullable Node node = ctrlApiDataLoader.loadNodeOrNull(nodeName);
        if (node == null)
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

        Peer nodePeer = getPeerPrivileged(node);
        if (nodePeer.isOnline())
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

        for (Resource rsc : getRscStreamPrivileged(node).collect(toList()))
        {
            ResourceDefinition rscDfn = rsc.getResourceDefinition();
            rscDfnToCheck.add(rscDfn);

            if (!isMarkedForDeletion(rsc))
            {
                for (Resource peerRsc : getRscStreamPrivileged(rscDfn).collect(toList()))
                {
                    nodesToContact.put(peerRsc.getNode().getName(), peerRsc.getNode());
                }
            }
            ctrlRscDeleteApiHelper.cleanupAndDelete(rsc);
        }
        // make sure that the just "lost" node is not contacted
        nodesToContact.remove(nodeName);

        // set node mark deleted for updates to other satellites
        markDeleted(node);

        Collection<Snapshot> snapshots = new ArrayList<>(getSnapshotsPrivileged(node));
        for (Snapshot snapshot : snapshots)
        {
            SnapshotDefinition snapDfn = snapshot.getSnapshotDefinition();
            deletePrivileged(snapshot);
            deleteSnapDfnPrivilegedIfNeeded(snapDfn);
        }

        // If the node has no resources or snapshots, then there should not be any volumes referenced
        // by the storage pool -- double check and delete storage pools
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
                        nodeNameStr,
                        storPool.getName().displayValue
                    )
                ));
            }
        }

        String successMessage = firstLetterCaps(getNodeDescriptionInline(nodeNameStr)) + " deleted.";
        UUID nodeUuid = node.getUuid(); // store node uuid to avoid deleted node access

        deletePrivileged(node);

        removeNodePrivileged(nodeName);

        ctrlTransactionHelper.commit();

        reconnectorTask.removePeer(nodePeer);

        // It may be possible to continue some operations since we are no longer waiting for the node to come online
        Flux<?> operationContinuation = Flux.merge(
            rscDfnToCheck.stream()
                .map(rscDfn -> ctrlSatelliteConnectionNotifier.checkResourceDefinitionConnected(rscDfn, context))
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
                    errorReporter,
                    updateResponses,
                    (String) null,
                    "Notified {0} that ''" + nodeName + "'' has been lost"
                ))
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());

        return Flux
            .just(responses)
            .concatWith(satelliteUpdateResponses)
            .concatWith(operationContinuation.thenMany(Flux.empty()));
    }

    private void deleteSnapDfnPrivilegedIfNeeded(SnapshotDefinition snapDfn)
    {
        try
        {
            if (snapDfn.getAllSnapshots().isEmpty())
            {
                snapDfn.delete();
            }
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void requireNodesMapChangeAccess()
    {
    }

    private Peer getPeerPrivileged(Node node)
    {
        Peer nodePeer;
        nodePeer = node.getPeer();
        return nodePeer;
    }

    private boolean hasVolumesPrivileged(StorPool storPool)
    {
        boolean hasVolumes;
        hasVolumes = !storPool.getVolumes().isEmpty();
        return hasVolumes;
    }

    private Stream<Resource> getRscStreamPrivileged(Node node)
    {
        Stream<Resource> stream;
        stream = node.streamResources();
        return stream;
    }

    private Stream<Resource> getRscStreamPrivileged(ResourceDefinition rscDfn)
    {
        Stream<Resource> stream;
        stream = rscDfn.streamResource();
        return stream;
    }

    private Iterator<StorPool> getStorPoolIteratorPrivileged(Node node)
    {
        Iterator<StorPool> iterateStorPools;
        // Shallow-copy the storage pool map, because the Iterator is used for
        // Node.delete(), which removes objects from the original map
        Map<StorPoolName, StorPool> storPoolMap = new TreeMap<>();
        node.copyStorPoolMap(storPoolMap);

        iterateStorPools = storPoolMap.values().iterator();
        return iterateStorPools;
    }

    private Collection<Snapshot> getSnapshotsPrivileged(Node node)
    {
        Collection<Snapshot> snapshots;
        snapshots = node.getSnapshots();
        return snapshots;
    }

    private boolean isMarkedForDeletion(Resource rsc)
    {
        boolean isMarkedForDeletion;
        isMarkedForDeletion = rsc.getStateFlags().isSet(Resource.Flags.DELETE);
        return isMarkedForDeletion;
    }

    private void markDeleted(Node node)
    {
        try
        {
            node.markDeleted();
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
            Node.Type nodeType = node.getNodeType();
            if (nodeType.isSpecial())
            {
                Integer port = specTargetProcMgr.stopProcess(node);
                if (port != null)
                {
                    specStltPortPool.deallocate(port);
                }
            }
            node.delete();
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
            storPool.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void deletePrivileged(Snapshot snapshot)
    {
        try
        {
            snapshot.delete();
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void removeNodePrivileged(NodeName nodeName)
    {
        nodeRepository.remove(nodeName);
    }
}
