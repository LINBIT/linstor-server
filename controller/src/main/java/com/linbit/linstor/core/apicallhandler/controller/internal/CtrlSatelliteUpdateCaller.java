package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.SatelliteConnectorImpl;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.StorPool;
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
    private final Provider<CtrlAuthenticator> ctrlAuthenticator;

    @Inject
    private CtrlSatelliteUpdateCaller(
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer serializerRef,
        Provider<RetryResourcesTask> retryResourceTaskProviderRef,
        SatelliteConnectorImpl stltConnectorRef,
        Provider<CtrlAuthenticator> ctrlAuthenticatorRef
    )
    {
        apiCtx = apiCtxRef;
        internalComSerializer = serializerRef;
        retryResourceTaskProvider = retryResourceTaskProviderRef;
        stltConnector = stltConnectorRef;
        ctrlAuthenticator = ctrlAuthenticatorRef;
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
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        Resource rsc,
        Publisher<ApiCallRc> nextStepRef
    )
    {
        return updateSatellites(rsc.getDefinition(), nextStepRef);
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        ResourceDefinition rscDfn,
        Publisher<ApiCallRc> nextStepRef
    )
    {
        return updateSatellites(rscDfn, notConnectedWarn(), nextStepRef);
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, Flux<ApiCallRc>>> updateSatellites(
        ResourceDefinition rscDfn,
        NotConnectedHandler notConnectedHandler,
        Publisher<ApiCallRc> nextStep
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

                Flux<ApiCallRc> response = updateResource(currentRsc, notConnectedHandler, nextStep);

                responses.add(Tuples.of(currentRsc.getNode().getName(), response));
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
        Publisher<ApiCallRc> nextStepRef
    )
        throws AccessDeniedException
    {
        Node node = currentRsc.getNode();
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
        return nodeName -> Flux.error(new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName)));
    }

    public static NotConnectedHandler notConnectedError()
    {
        return nodeName -> Flux.error(new ApiRcException(ApiCallRcImpl.simpleEntry(
            ApiConsts.FAIL_NOT_CONNECTED,
            "Connection to satellite '" + nodeName + "' lost"
        )));
    }

    public Flux<Boolean> attemptConnecting(AccessContext accCtx, Node nodeRef, long timeoutMillis)
    {
        return Flux.<Boolean>create(fluxSink ->
            {
                nodeRef.registerInitialConnectSink(fluxSink);
                stltConnector.startConnecting(nodeRef, accCtx, false);
            }
        )
        .timeout(
            Duration.ofMillis(timeoutMillis),
            Flux.just(false)
        );
    }
}
