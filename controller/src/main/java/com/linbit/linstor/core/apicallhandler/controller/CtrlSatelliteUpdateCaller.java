package com.linbit.linstor.core.apicallhandler.controller;

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
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Notifies satellites of updates, returning the responses from the deployment of these changes.
 * When failures occur (either due to connection problems or deployment failures) they are converted into responses
 * and a {@link DelayedApiRcException} is emitted after all the updates have terminated.
 */
@Singleton
public class CtrlSatelliteUpdateCaller
{
    private final AccessContext apiCtx;
    private final CtrlStltSerializer internalComSerializer;

    @Inject
    private CtrlSatelliteUpdateCaller(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlStltSerializer serializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef
    )
    {
        apiCtx = apiCtxRef;
        internalComSerializer = serializerRef;
    }

    /**
     * @param uuid UUID of changed node
     * @param nodeName Name of changed node
     * @param nodesToContact Nodes to update
     */
    public Flux<Tuple2<NodeName, ApiCallRc>> updateSatellites(
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
                if (peer != null && peer.isConnected())
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

        return mergeExtractingApiRcExceptions(Flux.fromIterable(responses));
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
    public Flux<Tuple2<NodeName, ApiCallRc>> updateSatellites(Resource rsc)
    {
        return updateSatellites(rsc.getDefinition());
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public Flux<Tuple2<NodeName, ApiCallRc>> updateSatellites(ResourceDefinition rscDfn)
    {
        List<Tuple2<NodeName, Flux<ApiCallRc>>> responses = new ArrayList<>();

        try
        {
            // notify all peers that one of their resources has changed
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();

                Flux<ApiCallRc> response = updateResource(currentRsc);

                responses.add(Tuples.of(currentRsc.getAssignedNode().getName(), response));
            }
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }

        return mergeExtractingApiRcExceptions(Flux.fromIterable(responses));
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

    public Flux<Tuple2<NodeName, ApiCallRc>> updateSatellites(
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

        return mergeExtractingApiRcExceptions(Flux.fromIterable(responses));
    }

    private Flux<ApiCallRc> updateResource(Resource currentRsc)
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

                .onErrorMap(PeerNotConnectedException.class, ignored ->
                    new ApiRcException(ResponseUtils.makeNotConnectedWarning(nodeName))
                );
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
                MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse =
                    MsgApiCallResponseOuterClass.MsgApiCallResponse.parseDelimitedFrom(inputStream);
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

    /**
     * Merge the sources, delaying failure.
     * Any {@link ApiRcException} errors are suppressed and converted into normal responses.
     * If any errors were suppressed, a token {@link DelayedApiRcException} error is emitted when all sources complete.
     */
    private static Flux<Tuple2<NodeName, ApiCallRc>> mergeExtractingApiRcExceptions(
        Publisher<Tuple2<NodeName, ? extends Publisher<ApiCallRc>>> sources)
    {
        return Flux
            .merge(
                Flux.from(sources)
                    .map(source ->
                        Flux.from(source.getT2())
                            .map(Signal::next)
                            .onErrorResume(ApiRcException.class, error -> Flux.just(Signal.error(error)))
                            .map(signal -> Tuples.of(source.getT1(), signal))
                    )
            )
            .compose(signalFlux ->
                {
                    List<ApiRcException> errors = Collections.synchronizedList(new ArrayList<>());
                    return signalFlux
                        .map(namedSignal ->
                            {
                                Signal<ApiCallRc> signal = namedSignal.getT2();
                                ApiCallRc apiCallRc;
                                if (signal.isOnError())
                                {
                                    ApiRcException apiRcException = (ApiRcException) signal.getThrowable();
                                    errors.add(apiRcException);
                                    apiCallRc = apiRcException.getApiCallRc();
                                }
                                else
                                {
                                    apiCallRc = signal.get();
                                }
                                return Tuples.of(namedSignal.getT1(), apiCallRc);
                            }
                        )
                        .concatWith(Flux.defer(() ->
                            errors.isEmpty() ?
                                Flux.empty() :
                                Flux.error(new DelayedApiRcException(errors))
                        ));
                }
            );
    }

    /**
     * See {@link CtrlSatelliteUpdateCaller}.
     */
    public static class DelayedApiRcException extends RuntimeException
    {
        private final List<ApiRcException> errors;

        public DelayedApiRcException(List<ApiRcException> errorsRef)
        {
            super("Exceptions have been converted to responses");
            errors = errorsRef;
        }

        public List<ApiRcException> getErrors()
        {
            return errors;
        }
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
}
