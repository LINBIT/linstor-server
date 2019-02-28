package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass.ApiCallResponse;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.RetryResourcesTask;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Notifies satellites of updates, returning the responses from the deployment of these changes.
 */
@Singleton
public class CtrlSatelliteUpdateCaller
{
    private final AccessContext apiCtx;
    private final CtrlStltSerializer internalComSerializer;
    private RetryResourcesTask retryResourceTask;

    @Inject
    private CtrlSatelliteUpdateCaller(
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer serializerRef,
        RetryResourcesTask retryResourceTaskRef
    )
    {
        apiCtx = apiCtxRef;
        internalComSerializer = serializerRef;
        retryResourceTask = retryResourceTaskRef;
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
                if (peer != null && peer.getConnectionStatus() == Peer.ConnectionStatus.ONLINE)
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
        Flux<ApiCallRc> response;
        Peer peer = satelliteToUpdate.getPeer(apiCtx);

        if (peer.isConnected() && peer.hasFullSyncFailed())
        {
            response = Flux.error(new ApiRcException(ResponseUtils.makeFullSyncFailedResponse(peer)));
        }
        else
        {
            NodeName nodeName = satelliteToUpdate.getName();

            response = peer
                .apiCall(
                    InternalApiConsts.API_CHANGED_NODE,
                    changedMessage
                )

                .map(inputStream -> deserializeApiCallRc(nodeName, inputStream))

                .onErrorMap(PeerNotConnectedException.class, ignored ->
                    new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))
                );
        }

        return response;
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(Resource rsc)
    {
        return updateSatellites(rsc.getDefinition());
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(ResourceDefinition rscDfn)
    {
        return updateSatellites(rscDfn, notConnectedWarn());
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        ResourceDefinition rscDfn,
        NotConnectedHandler notConnectedHandler
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

                Flux<ApiCallRc> response = updateResource(currentRsc, notConnectedHandler);

                responses.add(Tuples.of(currentRsc.getAssignedNode().getName(), response));
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
        final Node node = storPool.getNode();
        NodeName nodeName = node.getName();

        Flux<ApiCallRc> response;

        try
        {
            Peer currentPeer = node.getPeer(apiCtx);

            if (currentPeer.isConnected() && currentPeer.hasFullSyncFailed())
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
                                storPool.getUuid(),
                                storPool.getName().displayValue
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
        NotConnectedHandler notConnectedHandler
    )
        throws AccessDeniedException
    {
        Node node = currentRsc.getAssignedNode();
        NodeName nodeName = node.getName();

        Flux<ApiCallRc> response;
        Peer currentPeer = node.getPeer(apiCtx);

        if (currentPeer.isConnected() && currentPeer.hasFullSyncFailed())
        {
            response = Flux.error(new ApiRcException(ResponseUtils.makeFullSyncFailedResponse(currentPeer)));
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
                            currentRsc.getDefinition().getName().displayValue
                        )
                        .build()
                )

                .map(inputStream -> deserializeApiCallRc(nodeName, inputStream))

                .onErrorResume(
                    PeerNotConnectedException.class,
                    ignored -> notConnectedHandler.handleNotConnected(nodeName)
                )
                .doOnError(ignored -> retryResourceTask.add(currentRsc))
                .doOnComplete(() -> retryResourceTask.remove(currentRsc));
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

        if (currentPeer.isConnected() && currentPeer.hasFullSyncFailed())
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

        return response;
    }

    private ApiCallRc deserializeApiCallRc(NodeName nodeName, ByteArrayInputStream inputStream)
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
        return nodeName -> Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName)));
    }

    public static NotConnectedHandler notConnectedError()
    {
        return nodeName -> Flux.error(new ApiRcException(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_NOT_CONNECTED,
            "Connection to satellite '" + nodeName + "' lost"
        )));
    }
}
