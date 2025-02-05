package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.responses.MsgErrorReportOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Singleton
public class CtrlErrorListApiCallHandler
{
    private final ScopeRunner scopeRunner;
    private final ErrorReporter errorReporter;
    private final NodeRepository nodeRepository;
    private final CtrlStltSerializer stltComSerializer;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final String nodeNameForErrorReports;

    @Inject
    public CtrlErrorListApiCallHandler(
        ErrorReporter errorReporterRef,
        NodeRepository nodeRepositoryRef,
        CtrlStltSerializer clientComSerializerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef)
    {
        errorReporter = errorReporterRef;
        nodeRepository = nodeRepositoryRef;
        stltComSerializer = clientComSerializerRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        nodeNameForErrorReports = LinStor.getHostName();
    }

    public Flux<ApiCallRc> deleteErrorReports(
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final List<String> nodes,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Delete error reports on nodes",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                () -> assembleDeleteRequests(since, to, nodes, exception, version, ids))
            .collectList()
            .flatMapMany(deleteAnswer ->
                scopeRunner.fluxInTransactionalScope(
                    "Delete error report on controller and build api answers",
                    lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP),
                    () -> assembleDeleteRcs(since, to, nodes, exception, version, ids, deleteAnswer)
                ));
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleDeleteRequests(
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final List<String> nodes,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
        throws AccessDeniedException
    {
        Set<String> nodesFilter = nodes != null ?
            nodes.stream().map(String::toLowerCase).collect(Collectors.toSet()) : Collections.emptySet();
        final Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream();

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeStream
            .filter(n -> nodesFilter.isEmpty() || nodesFilter.contains(n.getName().displayValue.toLowerCase()))
            .map(node -> Tuples.of(node.getName(), prepareErrDelReq(node, since, to, exception, version, ids)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream)));
    }

    private Flux<ByteArrayInputStream> prepareErrDelReq(
        final Node node,
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> fluxReturn = Flux.empty();
        if (peer != null)
        {
            byte[] msg = stltComSerializer.headerlessBuilder()
                .deleteErrorReports(since, to, exception, version, ids).build();
            fluxReturn = peer.apiCall(ApiConsts.API_DEL_ERROR_REPORT, msg)
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        return fluxReturn;
    }

    private Flux<ApiCallRc> assembleDeleteRcs(
        @Nullable final Date since,
        @Nullable final Date to,
        @Nullable final List<String> nodes,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids,
        List<Tuple2<NodeName, ByteArrayInputStream>> deleteAnswers)
        throws IOException
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        if (nodes == null ||
            nodes.isEmpty() ||
            nodes.stream().anyMatch(n -> n.equalsIgnoreCase(LinStor.CONTROLLER_MODULE)))
        {
            // delete on controller
            apiCallRc.addEntries(
                errorReporter.deleteErrorReports(
                    since,
                    to,
                    exception,
                    version,
                    ids
                )
            );
        }

        // Returned satellite error deletion answers
        for (Tuple2<NodeName, ByteArrayInputStream> deleteAnswer : deleteAnswers)
        {
            NodeName nodeName = deleteAnswer.getT1();
            ByteArrayInputStream dataIn = deleteAnswer.getT2();

            ApiCallRc nodeApis = ProtoDeserializationUtils.parseApiCallAnswerMsg(dataIn, nodeName.displayValue + ": ");
            nodeApis.forEach(entry -> entry.getObjRefs().put(ApiConsts.KEY_NODE, nodeName.displayValue));
            apiCallRc.addEntries(nodeApis);
        }
        if (apiCallRc.isEmpty())
        {
            apiCallRc.addEntry("No error reports deleted.", ApiConsts.INFO_NOOP);
        }
        return Flux.just(apiCallRc);
    }

    public Flux<ErrorReportResult> listErrorReports(
        final Set<String> nodes,
        boolean withContent,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Collect error reports",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                () -> assembleRequests(nodes, withContent, since, to, ids, limit, offset)
            )
            .collectList()
            .flatMapMany(errorReportAnswers ->
                scopeRunner.fluxInTransactionlessScope(
                    "Assemble error report list",
                    lockGuardFactory.buildDeferred(LockType.READ), // no lock needed
                    () -> Flux.just(assembleList(nodes, withContent, since, to, ids, limit, offset, errorReportAnswers))
                )
            );
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(
        Set<String> nodesToRequest,
        boolean withContent,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset)
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream()
            .filter(node -> nodesToRequest.isEmpty() ||
                nodesToRequest.stream().anyMatch(node.getName().getDisplayName()::equalsIgnoreCase));

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeStream
            .map(node ->
                Tuples.of(node.getName(), prepareErrRequestApi(node, withContent, since, to, ids, limit, offset)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream)));
    }

    private Flux<ByteArrayInputStream> prepareErrRequestApi(
        final Node node,
        boolean withContent,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset)
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> fluxReturn = Flux.empty();
        if (peer != null)
        {
            byte[] msg = stltComSerializer.headerlessBuilder()
                .requestErrorReports(new HashSet<>(), withContent, since, to, ids, limit, offset).build();
            fluxReturn = peer.apiCall(ApiConsts.API_REQ_ERROR_REPORTS, msg)
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        return fluxReturn;
    }

    private @Nullable Peer getPeer(Node node)
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

    /**
     * Gets the errorreport answers from satellites and merges them all together with the controller.
     *
     * @param nodesToRequest Set of which nodes needed to be requested.
     * @param withContent true if error-reports should include all text content
     * @param since only include error-reports since this date
     * @param to only include error-reports to this date
     * @param ids only include error-reports with the given ids
     * @param limit only fetch maximum count
     * @param offset skip error reports until this
     * @param errorReportsAnswers Actual binary answers from satellites
     * @return A combined ErrorReportResult from all requested nodes
     * @throws IOException If parsing binary data from satellites failed.
     */
    private ErrorReportResult assembleList(
        Set<String> nodesToRequest,
        boolean withContent,
        @Nullable final Date since,
        @Nullable final Date to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset,
        List<Tuple2<NodeName, ByteArrayInputStream>> errorReportsAnswers)
        throws IOException
    {
        final ErrorReportResult errorReportResult = new ErrorReportResult(0, Collections.emptyList());

        // Controller error reports
        if (nodesToRequest.isEmpty() || nodesToRequest.stream().anyMatch(LinStor.CONTROLLER_MODULE::equalsIgnoreCase))
        {
            errorReportResult.addErrorReportResult(
                nodeNameForErrorReports,
                Node.Type.CONTROLLER.name(),
                errorReporter.listReports(
                    withContent,
                    since,
                    to,
                    ids,
                    limit,
                    offset)
            );
        }

        // Returned satellite error reports
        for (Tuple2<NodeName, ByteArrayInputStream> errorReportAnswer : errorReportsAnswers)
        {
            // NodeName nodeName = errorReportAnswer.getT1();
            ByteArrayInputStream errorReportMsgDataIn = errorReportAnswer.getT2();

            errorReportResult.addErrorReportResult(
                errorReportAnswer.getT1().displayValue,
                Node.Type.SATELLITE.name(),
                deserializeErrorReports(errorReportMsgDataIn));
        }

        errorReportResult.sort();
        errorReporter.logInfo("Assembled error reports; count %d", errorReportResult.getErrorReports().size());
        return errorReportResult;
    }

    // TODO? hide deserialization in interface?
    private static ErrorReportResult deserializeErrorReports(InputStream msgDataIn)
        throws IOException
    {
        List<ErrorReport> errorReports = new ArrayList<>();
        MsgErrorReportOuterClass.MsgErrorReport msgErrorReport;
        msgErrorReport = MsgErrorReportOuterClass.MsgErrorReport.parseDelimitedFrom(msgDataIn);
        for (MsgErrorReportOuterClass.ErrorReport errorReport : msgErrorReport.getErrorReportsList())
        {
            errorReports.add(new ErrorReport(
                errorReport.getNodeNames(),
                Node.Type.getByValue(errorReport.getModule()),
                errorReport.getFilename(),
                errorReport.getVersion(),
                errorReport.getPeer(),
                errorReport.getException(),
                errorReport.getExceptionMessage(),
                errorReport.getOriginFile(),
                errorReport.getOriginMethod(),
                errorReport.getOriginLine(),
                new Date(errorReport.getErrorTime()),
                errorReport.getText())
            );
        }

        return new ErrorReportResult(msgErrorReport.getTotalCount(), errorReports);
    }
}
