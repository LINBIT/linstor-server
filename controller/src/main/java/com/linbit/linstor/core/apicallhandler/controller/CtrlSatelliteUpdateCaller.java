package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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

    public Flux<ApiCallRc> updateSatellites(Resource rsc)
    {
        return updateSatellites(rsc.getDefinition());
    }

    public Flux<ApiCallRc> updateSatellites(ResourceDefinition rscDfn)
    {
        List<Flux<ApiCallRc>> responses = new ArrayList<>();

        try
        {
            // notify all peers that one of their resources has changed
            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();

                Flux<ApiCallRc> response = updateResource(currentRsc);

                responses.add(response);
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
            response = Flux.error(new ApiRcException(makeFullSyncFailedResponse(currentPeer)));
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
                    new ApiRcException(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.WARN_NOT_CONNECTED,
                            "No active connection to satellite '" + nodeName.displayValue + "'"
                        )
                        .setDetails(
                            "The controller is trying to (re-) establish a connection to the satellite. " +
                                "The controller stored the changes and as soon the satellite is connected, it will " +
                                "receive this update."
                        )
                        .build()
                    )
                );
        }

        return response;
    }

    private static ApiCallRc.RcEntry makeFullSyncFailedResponse(Peer satellite)
    {
        return ApiCallRcImpl
            .entryBuilder(
                ApiConsts.WARN_STLT_NOT_UPDATED,
                "Satellite reported an error during fullSync. This change will NOT be " +
                    "delivered to satellte '" + satellite.getNode().getName().displayValue +
                    "' until the error is resolved. Reconnect the satellite to the controller " +
                    "to remove this blockade."
            )
            .build();
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
                ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
                entry.setReturnCode(apiCallResponse.getRetCode());
                entry.setMessage(
                    "(" + nodeName.displayValue + ") " + apiCallResponse.getMessage()
                );
                entry.setCause(apiCallResponse.getCause());
                entry.setCorrection(apiCallResponse.getCorrection());
                entry.setDetails(apiCallResponse.getDetails());
                entry.putAllObjRef(readLinStorMap(apiCallResponse.getObjRefsList()));
                deploymentState.addEntry(entry);
            }
        }
        catch (IOException exc)
        {
            throw new ImplementationError(exc);
        }

        return deploymentState;
    }

    private Map<String, String> readLinStorMap(List<LinStorMapEntryOuterClass.LinStorMapEntry> linStorMap)
    {
        return linStorMap.stream()
            .collect(Collectors.toMap(
                LinStorMapEntryOuterClass.LinStorMapEntry::getKey,
                LinStorMapEntryOuterClass.LinStorMapEntry::getValue
            ));
    }

    /**
     * Merge the sources, delaying failure.
     * Any {@link ApiRcException} errors are suppressed and converted into normal responses.
     * If any errors were suppressed, a token {@link DelayedApiRcException} error is emitted when all sources complete.
     */
    private static Flux<ApiCallRc> mergeExtractingApiRcExceptions(Publisher<? extends Publisher<ApiCallRc>> sources)
    {
        return Flux
            .merge(
                Flux.from(sources)
                    .map(source ->
                        Flux.from(source)
                            .map(Signal::next)
                            .onErrorResume(ApiRcException.class, error -> Flux.just(Signal.error(error)))
                    )
            )
            .compose(signalFlux ->
                {
                    AtomicBoolean hasError = new AtomicBoolean();
                    return signalFlux
                        .map(signal ->
                            {
                                ApiCallRc apiCallRc;
                                if (signal.isOnError())
                                {
                                    hasError.set(true);
                                    ApiRcException apiRcException = (ApiRcException) signal.getThrowable();
                                    apiCallRc = apiRcException.getApiCallRc();
                                }
                                else
                                {
                                    apiCallRc = signal.get();
                                }
                                return apiCallRc;
                            }
                        )
                        .concatWith(Flux.defer(() ->
                            hasError.get() ?
                                Flux.error(new DelayedApiRcException()) :
                                Flux.empty()
                        ));
                }
            );
    }

    public static class DelayedApiRcException extends RuntimeException
    {
        public DelayedApiRcException()
        {
            super("Exceptions have been converted to responses");
        }
    }
}
