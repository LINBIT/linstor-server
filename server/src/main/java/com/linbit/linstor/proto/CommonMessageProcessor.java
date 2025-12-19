package com.linbit.linstor.proto;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.BaseApiCall;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.api.protobuf.ApiCallDescriptor;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ResponseUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.MessageProcessor;
import com.linbit.linstor.netcom.MessageTypes;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader.MsgType;
import com.linbit.linstor.proto.common.ApiCallResponseOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.Authentication;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.locks.LockGuard;
import com.linbit.utils.MathUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.MDC;
import org.slf4j.event.Level;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Dispatcher for received messages
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class CommonMessageProcessor implements MessageProcessor
{
    private final ErrorReporter errorLog;
    private final ScopeRunner scopeRunner;
    private final CommonSerializer commonSerializer;

    private final Sinks.Many<Runnable> sink;

    private final Map<String, ApiEntry> apiCallMap;

    public static final int MIN_THR_COUNT = 4;
    public static final int MAX_THR_COUNT = 1024;
    public static final int MIN_QUEUE_SIZE = 4 * MIN_THR_COUNT;
    public static final int MAX_QUEUE_SIZE = 4 * MAX_THR_COUNT;
    public static final int THR_QUEUE_FACTOR = 4;

    @Inject
    public CommonMessageProcessor(
        ErrorReporter errorLogRef,
        Scheduler scheduler,
        ScopeRunner scopeRunnerRef,
        CommonSerializer commonSerializerRef,
        Map<String, BaseApiCall> apiCalls,
        Map<String, ApiCallDescriptor> apiCallDescriptors
    )
    {
        errorLog = errorLogRef;
        scopeRunner = scopeRunnerRef;
        commonSerializer = commonSerializerRef;

        int queueSize = MathUtils.bounds(
            MIN_QUEUE_SIZE,
            Math.min(LinStor.CPU_COUNT, MAX_THR_COUNT) * THR_QUEUE_FACTOR,
            MAX_QUEUE_SIZE
        );
        int thrCount = MathUtils.bounds(MIN_THR_COUNT, LinStor.CPU_COUNT, MAX_THR_COUNT);

        // Limit the number of messages that can be submitted for processing
        // concurrently by setting the processor's buffer size.
        // In the absence of any backpressure mechanism in the communications
        // protocol, we resort to blocking when too many messages are received and
        // letting the TCP buffer fill up.
        // Many messages from a single peer will still be queued in an unbounded
        // fashion as part of the message re-ordering.
        sink = Sinks.many().unicast().onBackpressureBuffer(Queues.<Runnable>unbounded(queueSize).get());
        Flux<Runnable> workerPool = sink.asFlux();
        Hooks.onOperatorError((throwable, context) -> {
            if (throwable instanceof CriticalError)
            {
                CriticalError.die(errorLog, (CriticalError) throwable);
            }
            return throwable;
        });
        workerPool
            .parallel(thrCount, 1)
            .runOn(scheduler, 1)
            .doOnNext(Runnable::run)
            .subscribe(
                ignored -> {
                    // do nothing
                }, exc -> errorLog.reportError(exc, null, null, "Uncaught exception in sink"));

        apiCallMap = new TreeMap<>();
        for (Map.Entry<String, BaseApiCall> entry : apiCalls.entrySet())
        {
            String apiName = entry.getKey();
            BaseApiCall apiCall = entry.getValue();
            ApiCallDescriptor apiDscr = apiCallDescriptors.get(apiName);
            if (apiDscr != null)
            {
                apiCallMap.put(apiName,
                    new ApiEntry(apiCall, apiDscr, apiDscr.requiresAuth(), apiDscr.transactional()));
            }
            else
            {
                errorLog.reportError(
                    Level.ERROR,
                    new ImplementationError(
                        ApiCallDescriptor.class.getSimpleName() + " entry is missing for API call object '" +
                            apiName + "'",
                        null
                    )
                );
            }
        }
    }

    /**
     * May be called on any thread.
     * For each peer, messages should be delivered in the same order as in the incoming stream.
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void processMessage(final Message msg, final TcpConnector connector, final Peer peer)
    {
        int msgType;
        try
        {
            peer.pongReceived();
            msgType = msg.getType();
            switch (msgType)
            {
                case MessageTypes.DATA:
                    long peerSeq = peer.getNextIncomingMessageSeq();

                    // Since reactor 3.4.x, it doesn't busy loop anymore itself, but rather let that be done by
                    // the library user see: https://github.com/reactor/reactor-core/issues/2049
                    Sinks.EmitResult emitRes = sink.tryEmitNext(
                        () -> this.doProcessMessage(msg, connector, peer, peerSeq));
                    while (emitRes == Sinks.EmitResult.FAIL_NON_SERIALIZED)
                    {
                        LockSupport.parkNanos(10);
                        emitRes = sink.tryEmitNext(() -> this.doProcessMessage(msg, connector, peer, peerSeq));
                    }
                    if (emitRes.isFailure())
                    {
                        errorLog.logError("Unable to emit processMessage");
                    }
                    break;
                case MessageTypes.PING:
                    peer.sendPong();
                    break;
                case MessageTypes.PONG:
                    // pongReceived is called for every case, making this case a no-op.
                    break;
                default:
                    String peerAddress = null;
                    int port = 0;
                    InetSocketAddress peerSocketAddr = peer.peerAddress();
                    if (peerSocketAddr != null)
                    {
                        peerAddress = peerSocketAddr.getAddress().toString();
                        port = peerSocketAddr.getPort();
                    }
                    // Reached when a message with an unknown message type is received
                    if (peerAddress != null)
                    {
                        errorLog.logDebug(
                            "Message of unknown type %d received on connector %s " +
                            "from peer at endpoint %s:%d",
                            msgType, peer.getConnectorInstanceName(), peerAddress, port
                        );
                    }
                    else
                    {
                        errorLog.logDebug(
                            "Message of unknown type %d received on connector %s " +
                                "from peer at unknown endpoint address",
                            msgType, peer.getConnectorInstanceName()
                        );
                    }
                    break;
            }
        }
        catch (IllegalMessageStateException exc)
        {
            errorLog.reportError(
                Level.ERROR,
                exc,
                peer.getAccessContext(),
                peer,
                null
            );
        }
    }

    /**
     * Called on a worker pool thread.
     */
    private void doProcessMessage(Message msg, TcpConnector connector, Peer peer, long peerSeq)
    {
        peer.processInOrder(peerSeq, Flux.defer(() ->
            peer.isConnected(false) ?
                this.doProcessInOrderMessage(msg, connector, peer, peerSeq) :
                Flux.empty()
        ));
    }

    /**
     * Called on a worker pool thread.
     * The messages from each peer are guaranteed to be delivered in the same order as in the incoming stream.
     * In particular, no two messages from a given peer will be processed at the same time.
     */
    private Flux<?> doProcessInOrderMessage(Message msg, TcpConnector connector, Peer peer, long peerSeq)
    {
        Flux<?> flux = Flux.empty();
        try
        {
            flux = handleDataMessage(msg, connector, peer, peerSeq)
                .doOnError(exc -> errorLog.reportError(
                    Level.ERROR,
                    exc,
                    peer.getAccessContext(),
                    peer,
                    null
                ))
                .onErrorResume(ignored -> Flux.empty());
        }
        catch (Exception | ImplementationError exc)
        {
            errorLog.reportError(
                Level.ERROR,
                exc,
                peer.getAccessContext(),
                peer,
                null
            );
        }
        return flux;
    }

    private Flux<?> handleDataMessage(
        final Message msg,
        final TcpConnector connector,
        final Peer peer,
        long peerSeq
    )
        throws IllegalMessageStateException, IOException
    {
        Flux<?> flux = Flux.empty();

        byte[] msgData = msg.getData();
        ByteArrayInputStream msgDataIn = new ByteArrayInputStream(msgData);

        MsgHeaderOuterClass.MsgHeader header = MsgHeaderOuterClass.MsgHeader.parseDelimitedFrom(msgDataIn);
        String apiCallLogId = header.hasApiCallId() ?
            String.format("%06x", header.getApiCallId()) : ErrorReporter.getNewLogId();
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, apiCallLogId))
        {
            MsgType msgType = header.getMsgType();

            switch (msgType)
            {
                case ONEWAY:
                    // fall-through
                case API_CALL:
                    flux = callApi(
                        connector, peer, header, msgDataIn, msgType == MsgType.API_CALL, peerSeq);
                    break;
                case ANSWER:
                    handleAnswer(peer, header, msgDataIn, peerSeq);
                    break;
                case COMPLETE:
                    handleComplete(peer, header, peerSeq);
                    break;
                default:
                    errorLog.logError(
                        "Message of unknown type " + msgType + " received"
                    );
                    break;
            }
        }

        return flux;
    }

    private long getApiCallId(MsgHeaderOuterClass.MsgHeader header)
    {
        if (!header.hasApiCallId())
        {
            throw new InvalidHeaderException("Expected API call ID not present (for '" + header.getMsgContent() + "')");
        }
        return header.getApiCallId();
    }

    private void handleAnswer(
        Peer peer,
        MsgHeaderOuterClass.MsgHeader header,
        ByteArrayInputStream msgDataIn,
        long peerSeq
    )
        throws IOException
    {
        long apiCallId = getApiCallId(header);
        errorLog.logTrace("Peer %s, API call %d answer received (seq %d)", peer, apiCallId, peerSeq);

        ApiRcException error = null;
        if (header.getMsgContent().equals(ApiConsts.API_REPLY))
        {
            // check for errors
            msgDataIn.mark(0);
            while (msgDataIn.available() > 0 && error == null)
            {
                ApiCallResponseOuterClass.ApiCallResponse apiCallResponse =
                    ApiCallResponseOuterClass.ApiCallResponse.parseDelimitedFrom(msgDataIn);
                if ((apiCallResponse.getRetCode() & ApiConsts.MASK_ERROR) == ApiConsts.MASK_ERROR)
                {
                    error = new ApiRcException(ProtoDeserializationUtils.parseApiCallRc(
                        apiCallResponse, "(" + peer.getNode().getName().displayValue + ") "
                    ));
                }
            }
            msgDataIn.reset();
        }

        if (!header.getMsgContent().equals(ApiConsts.API_END_OF_IMMEDIATE_ANSWERS))
        {
            if (error != null)
            {
                peer.apiCallError(apiCallId, error);
            }
            else
            {
                peer.apiCallAnswer(apiCallId, msgDataIn);
            }
        }
    }

    private void handleComplete(Peer peer, MsgHeaderOuterClass.MsgHeader header, long peerSeq)
    {
        long apiCallId = getApiCallId(header);
        errorLog.logTrace("Peer %s, API call %d complete received (seq %d)", peer, apiCallId, peerSeq);

        peer.apiCallComplete(apiCallId);
    }

    private Flux<?> callApi(
        TcpConnector connector,
        Peer peer,
        MsgHeaderOuterClass.MsgHeader header,
        ByteArrayInputStream msgDataIn,
        boolean respond,
        long peerSeq
    )
    {
        Flux<byte[]> messageFlux;
        String apiCallName = header.getMsgContent();

        String apiCallDescription = respond ? "API call " + getApiCallId(header) : "oneway call";
        errorLog.logDebug("Peer %s, %s '%s' start (seq %d)", peer, apiCallDescription, apiCallName, peerSeq);

        ApiEntry apiMapEntry = apiCallMap.get(apiCallName);
        String apicallLogId = MDC.get(ErrorReporter.LOGID);

        if (apiMapEntry != null)
        {
            AccessContext peerAccCtx = peer.getAccessContext();
            // API will execute
            // - if no authentication is required for that specific API
            // - if authentication is turned off globally by the security subsystem
            // - if the peer's access context has non-public (= authenticated) identity
            if (!(apiMapEntry.reqAuth && Authentication.isRequired()) ||
                peerAccCtx.subjectId != Identity.PUBLIC_ID)
            {
                Long apiCallId = respond ? getApiCallId(header) : 0L;

                messageFlux = execute(apiMapEntry, apiCallName, apiCallId, msgDataIn, respond)
                    .checkpoint("Fallback error handling wrapper")
                    .onErrorResume(
                        InvalidProtocolBufferException.class,
                        exc -> handleProtobufErrors(exc, apiCallName, peer, apiCallId)
                    )
                    .onErrorResume(
                        ApiRcException.class,
                        exc -> handleApiException(exc, peer, apiCallId)
                    )
                    .onErrorResume(
                        ApiAccessDeniedException.class,
                        exc -> handleApiAccessDeniedException(exc, peer, apiCallId)
                    )
                    .onErrorResume(
                        TransactionException.class,
                        exc -> handleTransactionException(exc, peer, apiCallId)
                    )
                    .contextWrite(Context.of(
                        ApiModule.API_CALL_NAME, apiCallName,
                        AccessContext.class, peerAccCtx,
                        Peer.class, peer,
                        ApiModule.API_CALL_ID, apiCallId
                    ));
            }
            else
            if (respond)
            {
                messageFlux = Flux.just(makeNotAuthorizedMessage(getApiCallId(header), apiCallName));
            }
            else
            {
                errorLog.reportError(
                    new ImplementationError(
                        "One way call was rejected because the peer is not authorized"
                    )
                );
                messageFlux = Flux.empty();
            }
        }
        else
        {
            errorLog.reportError(
                Level.DEBUG,
                new LinStorException(
                    "Non-existent API '" + apiCallName + "' called by the client",
                    "The API call '" + apiCallName + "' cannot be executed.",
                    "The specified API does not exist",
                    "- Correct the client application to call a supported API\n" +
                        "- Load the API module required by the client application into the server\n",
                    "The API call name specified by the client was:\n" +
                        apiCallName
                ),
                peer.getAccessContext(),
                peer,
                "The request was received on connector service '" + connector.getInstanceName() + "' " +
                    "of type '" + connector.getServiceName() + "'"
            );

            if (respond)
            {
                messageFlux = Flux.just(makeUnknownApiCallMessage(getApiCallId(header), apiCallName));
            }
            else
            {
                messageFlux = Flux.empty();
            }
        }

        Flux<byte[]> flux = respond ?
            Flux
                .merge(
                    messageFlux,
                    // Insert an API_END_OF_IMMEDIATE_ANSWERS message into the stream after the initial values from
                    // the main stream. This works because merge subscribes to and drains the available values from
                    // each of the sources in turn.
                    Flux.just(commonSerializer.answerBuilder(
                        ApiConsts.API_END_OF_IMMEDIATE_ANSWERS, getApiCallId(header)).build())
                )
                .onErrorResume(exc -> errorFallback(exc, peer, header))
                .concatWith(Flux.just(commonSerializer.completionBuilder(getApiCallId(header)).build()))
                .doOnNext(peer::sendMessage) :
            messageFlux
                .doOnNext(ignored ->
                    errorLog.logDebug("Dropping message generated for oneway call '" + apiCallName + "'"));

        return flux.doOnTerminate(() ->
        {
            MDC.put(ErrorReporter.LOGID, apicallLogId);
            errorLog.logDebug("Peer %s, %s '%s' end", peer, apiCallDescription, apiCallName);
        });
    }

    private Flux<byte[]> execute(
        ApiEntry apiMapEntry,
        String apiCallName,
        Long apiCallId,
        ByteArrayInputStream msgDataIn,
        boolean respond
    )
    {
        Flux<byte[]> flux;
        BaseApiCall apiObj = apiMapEntry.apiCall;
        final var logContextMap = MDC.getCopyOfContextMap();
        if (apiObj instanceof ApiCall)
        {
            flux = scopeRunner.fluxInScope(
                "Execute single-stage API " + apiCallName,
                LockGuard.createDeferred(),
                () -> executeNonReactive((ApiCall) apiObj, msgDataIn),
                apiMapEntry.transactional,
                logContextMap
            );
        }
        else
        if (apiObj instanceof ApiCallReactive)
        {
            Flux<byte[]> executionFlux = Mono
                .fromCallable(() -> {
                    MDC.setContextMap(logContextMap);
                    return ((ApiCallReactive) apiObj).executeReactive(msgDataIn);
                })
                .flatMapMany(Function.identity());

            flux = respond ?
                executionFlux.switchIfEmpty(Flux.just(makeNoResponseMessage(apiCallName, apiCallId))) :
                executionFlux;
        }
        else
        {
            throw new ImplementationError("API call of unknown type " + apiObj.getClass());
        }
        return flux;
    }

    private Flux<byte[]> executeNonReactive(ApiCall apiObj, ByteArrayInputStream msgDataIn)
        throws Exception
    {
        apiObj.execute(msgDataIn);
        return Flux.empty();
    }

    private Flux<byte[]> handleProtobufErrors(
        Throwable exc,
        String apiCallName,
        Peer peer,
        Long apiCallId
    )
    {
        String errorId = errorLog.reportError(
            Level.ERROR,
            exc,
            peer.getAccessContext(),
            peer,
            "Unable to parse protobuf protocol for '" + apiCallName + "'"
        );

        return Flux.just(makeProtobufErrorMessage(exc, apiCallId, apiCallName, errorId));
    }

    private Flux<byte[]> handleApiException(
        ApiRcException exc,
        Peer peer,
        Long apiCallId
    )
    {
        String errorId = errorLog.reportError(
            Level.ERROR,
            exc,
            peer.getAccessContext(),
            peer,
            exc.getMessage()
        );

        return Flux.just(makeApiErrorMessage(exc, apiCallId, errorId));
    }

    private Flux<byte[]> handleApiAccessDeniedException(
        ApiAccessDeniedException exc,
        Peer peer,
        Long apiCallId
    )
    {
        String message = ResponseUtils.getAccDeniedMsg(peer.getAccessContext(), exc.getAction());

        String errorId = errorLog.reportError(
            Level.ERROR,
            exc.getCause(),
            peer.getAccessContext(),
            peer,
            message
        );

        ApiCallRc.RcEntry entry = ApiCallRcImpl
            .entryBuilder(
                exc.getRetCode(),
                message
            )
            .setCause(exc.getCause().getMessage())
            .addErrorId(errorId)
            .build();

        return Flux.just(commonSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(entry)).build());
    }

    private Flux<byte[]> handleTransactionException(
        TransactionException exc,
        Peer peer,
        Long apiCallId
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseUtils.reportStatic(
            exc,
            exc.getMessage(),
            ApiConsts.FAIL_SQL,
            null,
            false,
            responses,
            errorLog,
            peer.getAccessContext(),
            peer
        );

        return Flux.just(commonSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(responses).build());
    }

    private Flux<byte[]> errorFallback(
        Throwable exc,
        Peer peer,
        MsgHeaderOuterClass.MsgHeader header
    )
    {
        String errorId = errorLog.reportError(
            exc,
            peer.getAccessContext(),
            peer,
            "Unhandled error executing API call '" + header.getMsgContent() + "'."
        );

        return Flux.just(makeUnhandledExceptionMessage(getApiCallId(header), header.getMsgContent(), exc, errorId));
    }

    private byte[] makeNotAuthorizedMessage(long apiCallId, String apiCallName)
    {
        return commonSerializer
            .answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.API_CALL_AUTH_REQ,
                    "The client is not authorized to execute the requested function call")
                .setCause(
                    "The requested function call can only be executed by an authenticated identity")
                .setDetails("The requested function call name was '" + apiCallName + "'.")
                .build()
            ))
            .build();
    }

    private byte[] makeProtobufErrorMessage(
        Throwable exc,
        Long apiCallId,
        String apiCallName,
        String errorId
    )
    {
        return commonSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.API_CALL_PARSE_ERROR, "Controller couldn't parse message.")
                .setCause(exc.getMessage())
                .setDetails("The requested function call name was '" + apiCallName + "'.")
                .addErrorId(errorId)
                .build()
            ))
            .build();
    }

    private byte[] makeApiErrorMessage(ApiRcException exc, Long apiCallId, String errorId)
    {
        ApiCallRc apiCallRc = exc.getApiCallRc();
        return commonSerializer.answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(new ApiCallRcImpl(apiCallRc.stream()
                .map(rcEntry ->
                    ApiCallRcImpl.entryBuilder(rcEntry, null, null)
                        .addErrorId(errorId)
                        .build()
                )
                .collect(Collectors.toList())
            ))
            .build();
    }

    private byte[] makeUnhandledExceptionMessage(
        long apiCallId,
        String apiCallName,
        Throwable exc,
        String errorId
    )
    {
        return commonSerializer
            .answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Failed with unhandled error")
                .setCause(exc.getMessage())
                .setDetails("In API call '" + apiCallName + "'.")
                .addErrorId(errorId)
                .build()
            ))
            .build();
    }

    private byte[] makeUnknownApiCallMessage(long apiCallId, String apiCallName)
    {
        String cause =
            "Common causes of this error are:\n" +
                "   - The function call name specified by the caller\n" +
                "     (client side) is incorrect\n" +
                "   - The requested function call was not loaded into\n" +
                "     the system (server side)";

        return commonSerializer
            .answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.UNKNOWN_API_CALL,
                    "The requested function call cannot be executed.")
                .setCause(cause)
                .setDetails("The requested function call name was '" + apiCallName + "'.")
                .build()
            ))
            .build();
    }

    private byte[] makeNoResponseMessage(String apiCallName, long apiCallId)
    {
        return commonSerializer
            .answerBuilder(ApiConsts.API_REPLY, apiCallId)
            .apiCallRcSeries(ApiCallRcImpl.singletonApiCallRc(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR,
                    "No response generated by handler.")
                .setDetails("In API call '" + apiCallName + "'.")
                .build()
            ))
            .build();
    }

    private static class ApiEntry
    {
        final BaseApiCall apiCall;
        final ApiCallDescriptor descriptor;
        final boolean reqAuth;
        final boolean transactional;

        ApiEntry(
            final BaseApiCall apiCallRef,
            final ApiCallDescriptor descriptorRef,
            final boolean reqAuthFlag,
            boolean transactionalRef
        )
        {
            apiCall = apiCallRef;
            descriptor = descriptorRef;
            reqAuth = reqAuthFlag;
            transactional = transactionalRef;
        }
    }

    private static class InvalidHeaderException extends RuntimeException
    {
        InvalidHeaderException(String message)
        {
            super(message);
        }
    }
}
