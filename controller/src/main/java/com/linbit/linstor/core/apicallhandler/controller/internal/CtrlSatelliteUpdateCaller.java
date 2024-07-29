package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.SatelliteConnectorImpl;
import com.linbit.linstor.core.apicallhandler.controller.internal.helpers.AtomicUpdateSatelliteData;
import com.linbit.linstor.core.apicallhandler.controller.req.CreateMultiSnapRequest;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.RetryResourcesTask;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.context.ContextView;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Notifies satellites of updates, returning the responses from the deployment of these changes.
 */
@Singleton
public class CtrlSatelliteUpdateCaller
{
    private final AccessContext apiCtx;
    private final CtrlStltSerializer internalComSerializer;
    private final Provider<RetryResourcesTask> retryResourceTaskProvider;
    private final SatelliteConnectorImpl stltConnector;
    private final NodeRepository nodeRepo;

    @Inject
    private CtrlSatelliteUpdateCaller(
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer serializerRef,
        Provider<RetryResourcesTask> retryResourceTaskProviderRef,
        SatelliteConnectorImpl stltConnectorRef,
        NodeRepository nodeRepoRef
    )
    {
        apiCtx = apiCtxRef;
        internalComSerializer = serializerRef;
        retryResourceTaskProvider = retryResourceTaskProviderRef;
        stltConnector = stltConnectorRef;
        nodeRepo = nodeRepoRef;
    }

    /**
     * @param uuid UUID of changed node
     * @param nodeName Name of changed node
     * @param nodesToContact Nodes to update
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        UUID uuid,
        NodeName nodeName,
        Collection<Node> nodesToContact
    )
    {
        List<Tuple2<NodeName, Flux<ApiCallRc>>> responses = new ArrayList<>();

        try
        {
            byte[] changedMessage = internalComSerializer
                .headerlessBuilder()
                .changedNode(
                    uuid,
                    nodeName.displayValue
                )
                .build();
            for (Node nodeToContact : nodesToContact)
            {
                Peer peer = nodeToContact.getPeer(apiCtx);
                if (peer != null && peer.getConnectionStatus() == ApiConsts.ConnectionStatus.ONLINE)
                {
                    Flux<ApiCallRc> response = updateSatellite(nodeToContact, changedMessage);

                    responses.add(Tuples.of(nodeToContact.getName(), response));
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return Flux.fromIterable(responses);
    }

    private Flux<ApiCallRc> updateSatellite(Node satelliteToUpdate, byte[] changedMessage)
        throws AccessDeniedException
    {
        return updateSatellite(satelliteToUpdate, InternalApiConsts.API_CHANGED_NODE, changedMessage);
    }

    private Flux<ApiCallRc> updateSatellite(Node satelliteToUpdate, String apiCallName, byte[] changedMessage)
        throws AccessDeniedException
    {
        Flux<ApiCallRc> response;
        Peer peer = satelliteToUpdate.getPeer(apiCtx);

        if (peer.isOnline() && peer.hasFullSyncFailed())
        {
            response = Flux.error(new ApiRcException(ResponseUtils.makeFullSyncFailedResponse(peer)));
        }
        else
        {
            NodeName nodeName = satelliteToUpdate.getName();

            response = peer.apiCall(apiCallName, changedMessage)
                .map(inputStream -> deserializeApiCallRc(nodeName, inputStream))
                .onErrorMap(
                    PeerNotConnectedException.class,
                    ignored -> new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))
                );
        }

        return response;
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        Resource rsc,
        Publisher<ApiCallRc> nextStepRef
    )
    {
        return updateSatellites(rsc.getResourceDefinition(), nextStepRef);
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        ResourceDefinition rscDfn,
        @Nullable Publisher<ApiCallRc> nextStepRef
    )
    {
        return Flux.deferContextual(cv -> updateSatellitesWithContext(rscDfn, nextStepRef, cv));
    }

    private Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellitesWithContext(
        ResourceDefinition rscDfn,
        @Nullable Publisher<ApiCallRc> nextStepRef,
        ContextView cv
    )
    {
        NotConnectedHandler dfltNotConnectedHandler;
        // TODO move this into context class
        if (cv != null &&
            cv.hasKey(InternalApiConsts.ONLY_WARN_IF_OFFLINE) &&
            Boolean.TRUE.equals(cv.get(InternalApiConsts.ONLY_WARN_IF_OFFLINE)))
        {
            dfltNotConnectedHandler = notConnectedWarn();
        }
        else
        {
            dfltNotConnectedHandler = notConnectedError();
        }

        return updateSatellites(rscDfn, dfltNotConnectedHandler, nextStepRef);
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        ResourceDefinition rscDfn,
        NotConnectedHandler notConnectedHandler,
        @Nullable Publisher<ApiCallRc> nextStep
    )
    {
        List<Tuple2<NodeName, Flux<ApiCallRc>>> responses = new ArrayList<>();

        try
        {
            // notify all peers that one of their resources has changed
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                if (!currentRsc.getNode().isEvicted(apiCtx))
                {
                    Flux<ApiCallRc> response = updateResource(currentRsc, notConnectedHandler, nextStep);

                    responses.add(Tuples.of(currentRsc.getNode().getName(), response));
                }
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return Flux.fromIterable(responses);
    }

    public Flux<ApiCallRc> updateSatellite(final StorPool storPool)
    {
        return updateSatellite(storPool.getUuid(), storPool.getName().displayValue, storPool.getNode());
    }

    public Flux<ApiCallRc> updateSatellite(final UUID storPoolUuid, final String storPoolName, final Node node)
    {
        NodeName nodeName = node.getName();

        Flux<ApiCallRc> response;

        try
        {
            Peer currentPeer = node.getPeer(apiCtx);

            if (currentPeer.isOnline() && currentPeer.hasFullSyncFailed())
            {
                response = Flux.error(new ApiRcException(ResponseUtils.makeFullSyncFailedResponse(currentPeer)));
            }
            else
            {
                response = currentPeer
                    .apiCall(
                        InternalApiConsts.API_CHANGED_STOR_POOL,
                        internalComSerializer
                            .headerlessBuilder()
                            .changedStorPool(
                                storPoolUuid,
                                storPoolName
                            )
                            .build()
                    )

                    .map(inputStream -> deserializeApiCallRc(nodeName, inputStream))

                    .onErrorMap(PeerNotConnectedException.class, ignored ->
                        new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))
                    );
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return response;
    }

    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        SnapshotDefinition snapshotDfn,
        NotConnectedHandler notConnectedHandler
    )
    {
        List<Tuple2<NodeName, Flux<ApiCallRc>>> responses = new ArrayList<>();

        try
        {
            // notify all peers that a snapshot has changed
            for (Snapshot snapshot : snapshotDfn.getAllSnapshots(apiCtx))
            {
                Flux<ApiCallRc> response = updateSnapshot(snapshot, notConnectedHandler);

                responses.add(Tuples.of(snapshot.getNodeName(), response));
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return Flux.fromIterable(responses);
    }

    private Flux<ApiCallRc> updateResource(
        Resource currentRsc,
        NotConnectedHandler notConnectedHandler,
        @Nullable Publisher<ApiCallRc> nextStepRef
    )
        throws AccessDeniedException
    {
        Node node = currentRsc.getNode();
        NodeName nodeName = node.getName();

        Flux<ApiCallRc> response;
        Peer currentPeer = node.getPeer(apiCtx);

        if (currentPeer.isOnline() && currentPeer.hasFullSyncFailed())
        {
            response = Flux.error(new ApiRcException(ResponseUtils.makeFullSyncFailedResponse(currentPeer)));
        }
        else if (!currentPeer.isOnline())
        {
            response = notConnectedHandler.handleNotConnected(nodeName);
        }
        else
        {
            response = currentPeer
                .apiCall(
                    InternalApiConsts.API_CHANGED_RSC,
                    internalComSerializer
                        .headerlessBuilder()
                        .changedResource(
                            currentRsc.getUuid(),
                            currentRsc.getResourceDefinition().getName().displayValue
                        )
                        .build()
                )

                .map(inputStream -> deserializeApiCallRc(nodeName, inputStream))

                .onErrorResume(
                    PeerNotConnectedException.class,
                    ignored -> notConnectedHandler.handleNotConnected(nodeName)
                )
                .doOnError(ignored -> retryResourceTaskProvider.get().add(currentRsc, nextStepRef));
        }

        return response;
    }

    private Flux<ApiCallRc> updateSnapshot(Snapshot snapshot, NotConnectedHandler notConnectedHandler)
        throws AccessDeniedException
    {
        Node node = snapshot.getNode();
        NodeName nodeName = node.getName();

        Flux<ApiCallRc> response;
        Peer currentPeer = node.getPeer(apiCtx);

        if (currentPeer == null)
        {
            // might be null if controller is just starting and establishing connection
            // to the first node which triggers an update to a all resources / snapshots
            // of a rsc-/snap-dfn, even to an rsc/snap with a node to which the controller
            // did not even attempted to connect -> null as peer
            response = notConnectedHandler.handleNotConnected(nodeName);
        }
        else
        {
            if (currentPeer.isOnline() && currentPeer.hasFullSyncFailed())
            {
                response = Flux.error(new ApiRcException(ResponseUtils.makeFullSyncFailedResponse(currentPeer)));
            }
            else
            {
                response = currentPeer
                    .apiCall(
                        InternalApiConsts.API_CHANGED_IN_PROGRESS_SNAPSHOT,
                        internalComSerializer
                            .headerlessBuilder()
                            .changedSnapshot(
                                snapshot.getResourceName().displayValue,
                                snapshot.getUuid(),
                                snapshot.getSnapshotName().displayValue
                            )
                            .build()
                    )

                    .map(inputStream -> deserializeApiCallRc(nodeName, inputStream))
                    .onErrorResume(
                        PeerNotConnectedException.class,
                        ignored -> notConnectedHandler.handleNotConnected(nodeName)
                    );
            }
        }
        return response;
    }

    public static ApiCallRc deserializeApiCallRc(NodeName nodeName, ByteArrayInputStream inputStream)
    {
        ApiCallRcImpl deploymentState = new ApiCallRcImpl();

        try
        {
            while (inputStream.available() > 0)
            {
                ApiCallResponse apiCallResponse = ApiCallResponse.parseDelimitedFrom(inputStream);
                deploymentState.addEntry(ProtoDeserializationUtils.parseApiCallRc(
                    apiCallResponse, "(" + nodeName.displayValue + ") "
                ));
            }
        }
        catch (IOException exc)
        {
            throw new ImplementationError(exc);
        }

        return deploymentState;
    }

    @FunctionalInterface
    public interface NotConnectedHandler
    {
        Flux<ApiCallRc> handleNotConnected(NodeName nodeName);
    }

    public static NotConnectedHandler notConnectedWarn()
    {
        return nodeName -> Flux.just(new ApiCallRcImpl(ResponseUtils.makeNotConnectedWarning(nodeName)));
    }

    public static NotConnectedHandler notConnectedIgnoreIf(NodeName toggleNode)
    {
        return nodeName -> nodeName.equals(toggleNode) ?
            Flux.empty() : Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName)));
    }

    public static NotConnectedHandler notConnectedIgnoreIfNot(NodeName toggleNode)
    {
        return nodeName -> nodeName.equals(toggleNode) ?
            Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))) : Flux.empty();
    }

    public static NotConnectedHandler notConnectedErrorForNodesWarnForOthers(NodeName... errorNodeNames)
    {
        return nodeName -> {
            Flux<ApiCallRc> ret;
            boolean error = false;
            for (NodeName errNodeName : errorNodeNames)
            {
                if (nodeName.equals(errNodeName))
                {
                    error = true;
                    break;
                }
            }
            if (error)
            {
                ret = Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName)));
            }
            else
            {
                ret = Flux.just(new ApiCallRcImpl(ResponseUtils.makeNotConnectedWarning(nodeName)));
            }
            return ret;

        };
    }

    public static NotConnectedHandler notConnectedError()
    {
        return nodeName -> Flux.error(new ApiRcException(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_NOT_CONNECTED,
            "No connection to satellite '" + nodeName + "'",
            true
        )));
    }

    public Flux<Boolean> attemptConnecting(AccessContext accCtx, Node nodeRef, long timeoutMillis)
    {
        Object key = new Object();
        return Flux.<Boolean>create(fluxSink ->
            {
                nodeRef.registerInitialConnectSink(key, fluxSink);
                stltConnector.startConnecting(nodeRef, accCtx, false);
            }
        )
            .timeout(
                Duration.ofMillis(timeoutMillis),
                Flux.just(false)
            )
            .doFinally(ignoredSignal -> nodeRef.removeInitialConnectSink(key));
    }

    public Flux<ApiCallRc> updateSatellite(ExternalFile extFileRef)
    {
        return Flux.merge(
            updateAllSatellites(
                InternalApiConsts.API_CHANGED_EXTERNAL_FILE,
                internalComSerializer.headerlessBuilder()
                    .changedExtFile(extFileRef.getUuid(), extFileRef.getName().extFileName)
                    .build()
            ).map(tuple -> tuple.getT2())
        );
    }

    public Flux<ApiCallRc> updateSatellites(AbsRemote remoteRef)
    {
        return Flux.merge(
            updateAllSatellites(
                InternalApiConsts.API_CHANGED_REMOTE,
                internalComSerializer.headerlessBuilder()
                    .changedRemote(remoteRef.getUuid(), remoteRef.getName().displayValue)
                    .build()
            ).map(tuple -> tuple.getT2())
        );
    }

    public Flux<ApiCallRc> updateSatellitesConf()
    {
        return Flux.merge(
            updateAllSatellites(
                InternalApiConsts.API_CHANGED_CONTROLLER,
                internalComSerializer.headerlessBuilder()
                    .build()
            ).map(Tuple2::getT2)
        );
    }

    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateAllSatellites(String apiChangedNodeRef, byte[] message)
    {
        List<Tuple2<NodeName, Flux<ApiCallRc>>> responses = new ArrayList<>();
        try
        {
            for (Node nodeToContact : nodeRepo.getMapForView(apiCtx).values())
            {
                Peer satellitePeer = nodeToContact.getPeer(apiCtx);

                if (satellitePeer.isOnline() && !satellitePeer.hasFullSyncFailed())
                {
                    Flux<ApiCallRc> response = updateSatellite(
                        nodeToContact,
                        apiChangedNodeRef,
                        message
                    );

                    responses.add(Tuples.of(nodeToContact.getName(), response));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.fromIterable(responses);
    }

    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        CreateMultiSnapRequest reqRef,
        NotConnectedHandler notConnectedErrorRef
    )
    {
        return updateSatellites(
            new AtomicUpdateSatelliteData().addSnapDfns(reqRef.getCreatedSnapDfns()),
            notConnectedErrorRef
        );
    }

    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        AtomicUpdateSatelliteData atomicUpdateDataRef,
        NotConnectedHandler notConnectedErrorRef
    )
    {
        try
        {
            return updateSatellites(
                atomicUpdateDataRef.getInvolvedOnlineNodes(apiCtx),
                atomicUpdateDataRef,
                notConnectedErrorRef
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Sends the atomic update to all given nodes.
     *
     * Different AtomicUpdates to different nodes must be handled outside of this class (or we need a new method for
     * that which uses something like Map<Node, AtomitUpdateSatelliteData>)
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        Collection<Node> nodesRef,
        AtomicUpdateSatelliteData atomicUpdateDataRef,
        NotConnectedHandler notConnectedHandler
    )
    {
        byte[] changedMessage = internalComSerializer.headerlessBuilder()
            .changedData(atomicUpdateDataRef)
            .build();
        List<Tuple2<NodeName, Flux<ApiCallRc>>> responses = new ArrayList<>();
        try
        {
            for (Node node : nodesRef)
            {
                Peer peer = node.getPeer(apiCtx);
                if (peer != null && peer.getConnectionStatus() == ApiConsts.ConnectionStatus.ONLINE)
                {
                    NodeName nodeName = node.getName();

                    Flux<ApiCallRc> response = updateSatellite(
                        node,
                        InternalApiConsts.API_CHANGED_DATA,
                        changedMessage
                    ).onErrorResume(
                        PeerNotConnectedException.class,
                        ignored -> notConnectedHandler.handleNotConnected(nodeName)
                    );
                    // TODO we should also consider adding a retryTask for atomic updates

                    responses.add(Tuples.of(nodeName, response));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return Flux.fromIterable(responses);
    }
}
