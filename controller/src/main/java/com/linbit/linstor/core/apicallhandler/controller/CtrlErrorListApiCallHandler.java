package com.linbit.linstor.core.apicallhandler.controller;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.MsgErrorReportOuterClass;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class CtrlErrorListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final ErrorReporter errorReporter;
    private final NodeRepository nodeRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final Provider<AccessContext> peerAccCtx;
    private final ReadWriteLock nodesMapLock;
    private final Provider<Long> apiCallId;

    @Inject
    public CtrlErrorListApiCallHandler(
        ErrorReporter errorReporterRef,
        NodeRepository nodeRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef
    )
    {
        errorReporter = errorReporterRef;
        nodeRepository = nodeRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        nodesMapLock = nodesMapLockRef;
        apiCallId = apiCallIdRef;
    }

    public Flux<byte[]> listErrorReports(
        final Set<String> nodes,
        boolean withContent,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                LockGuard.createDeferred(nodesMapLock.readLock()),
                () -> assembleRequests(nodes, withContent, since, to, ids)
            )
            .collect(Collectors.toList())
            .flatMapMany(errorReportAnswers ->
                scopeRunner.callInTransactionlessScope(
                    LockGuard.createDeferred(nodesMapLock.readLock()),
                    () -> assembleList(nodes, withContent, since, to, ids, errorReportAnswers)
                )
            );
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(
        Set<String> nodesToRequest,
        boolean withContent,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids)
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream()
            .filter(node -> nodesToRequest.isEmpty() ||
                nodesToRequest.stream().anyMatch(node.getName().getDisplayName()::equalsIgnoreCase));

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeStream
            .map(node -> Tuples.of(node.getName(), prepareErrRequestApi(node, withContent, since, to, ids)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream)));
    }

    private Flux<ByteArrayInputStream> prepareErrRequestApi(
        final Node node,
        boolean withContent,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids)
    {
        Peer peer = getPeer(node);
        byte[] msg = clientComSerializer.headerlessBuilder()
            .requestErrorReports(new HashSet<>(), withContent, since, to, ids).build();

        return peer.apiCall(ApiConsts.API_REQ_ERROR_REPORTS, msg)
            .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        try
        {
            peer = node.getPeer(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access peer for node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return peer;
    }

    private byte[] assembleList(
        Set<String> nodesToRequest,
        boolean withContent,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids,
        List<Tuple2<NodeName, ByteArrayInputStream>> errorReportsAnswers)
        throws IOException
    {
        Set<ErrorReport> errorReports = new TreeSet<>();

        // Controller error reports
        if (nodesToRequest.isEmpty() || nodesToRequest.stream().anyMatch(Controller.MODULE::equalsIgnoreCase))
        {
            errorReports.addAll(StdErrorReporter.listReports(
                Controller.MODULE,
                errorReporter.getLogDirectory(),
                withContent,
                since,
                to,
                ids
            ));
        }

        // Returned satellite error reports
        for (Tuple2<NodeName, ByteArrayInputStream> errorReportAnswer : errorReportsAnswers)
        {
            // NodeName nodeName = errorReportAnswer.getT1();
            ByteArrayInputStream errorReportMsgDataIn = errorReportAnswer.getT2();

            errorReports.addAll(deserializeErrorReports(errorReportMsgDataIn));
        }

        // send combined answer
        return clientComSerializer.answerBuilder(ApiConsts.API_LST_ERROR_REPORTS, apiCallId.get())
            .errorReports(errorReports)
            .build();
    }

    // TODO? hide deserialization in interface?
    private static Set<ErrorReport> deserializeErrorReports(InputStream msgDataIn)
        throws IOException
    {
        MsgErrorReportOuterClass.MsgErrorReport msgErrorReport;
        Set<ErrorReport> errorReports = new TreeSet<>();
        while ((msgErrorReport = MsgErrorReportOuterClass.MsgErrorReport.parseDelimitedFrom(msgDataIn)) != null)
        {
            errorReports.add(new ErrorReport(
                msgErrorReport.getNodeNames(),
                msgErrorReport.getFilename(),
                new Date(msgErrorReport.getErrorTime()),
                msgErrorReport.getText())
            );
        }
        return errorReports;
    }
}
