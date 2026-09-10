package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReportResult;
import com.linbit.linstor.logging.ErrorReportSortBy;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.responses.MsgErrorReportOuterClass;
import com.linbit.utils.Pair;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    private final LockGuardFactory lockGuardFactory;
    private final String nodeNameForErrorReports;

    @Inject
    public CtrlErrorListApiCallHandler(
        ErrorReporter errorReporterRef,
        NodeRepository nodeRepositoryRef,
        CtrlStltSerializer clientComSerializerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef)
    {
        errorReporter = errorReporterRef;
        nodeRepository = nodeRepositoryRef;
        stltComSerializer = clientComSerializerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        nodeNameForErrorReports = LinStor.getHostName();
    }

    public Flux<ApiCallRc> deleteErrorReports(
        @Nullable final Instant since,
        @Nullable final Instant to,
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
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final List<String> nodes,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        Set<String> nodesFilter = nodes != null ?
            nodes.stream().map(String::toLowerCase).collect(Collectors.toSet()) : Collections.emptySet();
        final Stream<Node> nodeStream = nodeRepository.getMapForView().values().stream();

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
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final String exception,
        @Nullable final String version,
        @Nullable final List<String> ids)
    {
        byte[] msg = stltComSerializer.headerlessBuilder()
            .deleteErrorReports(since, to, exception, version, ids)
            .build();
        return getPeer(node).apiCall(ApiConsts.API_DEL_ERROR_REPORT, msg)
            .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> assembleDeleteRcs(
        @Nullable final Instant since,
        @Nullable final Instant to,
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
        @Nullable final Instant since,
        @Nullable final Instant to,
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

    /**
     * Lists one page of the globally sorted error reports of the requested nodes.
     * <p>
     * Every requested node is asked for its first {@code offset + limit} reports in the requested
     * sort order (metadata only), the answers are merge-sorted with the same ordering and reduced
     * to the requested page; the per-node total counts are summed up independently of the paging.
     * If {@code withContent} is set, the report texts are fetched in a second round-trip for the
     * reports of the returned page only.
     *
     * @param nodes Set of node names to request, empty means all nodes including the controller.
     * @param withContent true if the reports of the returned page should include their text
     * @param since only include error-reports since this date
     * @param to only include error-reports up to this date
     * @param moduleFilter only include reports of this module (controller/satellite), null for all
     * @param limit maximum number of reports in the returned page
     * @param offset number of reports of the globally sorted result to skip
     * @param sortBy field to sort by
     * @param sortAsc true for ascending, false for descending sort
     * @return A single ErrorReportResult holding the requested page and the total count
     */
    public Flux<ErrorReportResult> listErrorReportsPage(
        final Set<String> nodes,
        boolean withContent,
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final Node.Type moduleFilter,
        final long limit,
        final long offset,
        final ErrorReportSortBy sortBy,
        final boolean sortAsc
    )
    {
        return scopeRunner
            .fluxInTransactionlessScope(
                "Collect error report page",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                () -> moduleFilter == Node.Type.CONTROLLER ?
                    Flux.empty() :
                    assembleRequests(
                        nodes, false, since, to, Collections.emptySet(), offset + limit, 0L, sortBy, sortAsc)
            )
            .collectList()
            .flatMapMany(errorReportAnswers ->
                scopeRunner.fluxInTransactionlessScope(
                    "Assemble error report page",
                    lockGuardFactory.buildDeferred(LockType.READ), // no lock needed
                    () -> Flux.just(
                        assemblePage(nodes, since, to, moduleFilter, limit, offset, sortBy, sortAsc, errorReportAnswers)
                    )
                )
            )
            .flatMap(page -> withContent ? fetchPageTexts(page, nodes) : Flux.just(page));
    }

    private ErrorReportResult assemblePage(
        Set<String> nodesToRequest,
        @Nullable final Instant since,
        @Nullable final Instant to,
        @Nullable final Node.Type moduleFilter,
        final long limit,
        final long offset,
        final ErrorReportSortBy sortBy,
        final boolean sortAsc,
        List<Tuple2<NodeName, ByteArrayInputStream>> errorReportsAnswers
    )
        throws IOException
    {
        final ErrorReportResult result = new ErrorReportResult(0, Collections.emptyList());

        boolean includeController = moduleFilter == null || moduleFilter == Node.Type.CONTROLLER;
        if (includeController &&
            (nodesToRequest.isEmpty() || nodesToRequest.stream().anyMatch(LinStor.CONTROLLER_MODULE::equalsIgnoreCase)))
        {
            result.addErrorReportResult(
                nodeNameForErrorReports,
                Node.Type.CONTROLLER.name(),
                errorReporter.listReports(
                    false,
                    since,
                    to,
                    Collections.emptySet(),
                    offset + limit,
                    0L,
                    sortBy,
                    sortAsc)
            );
        }

        for (Tuple2<NodeName, ByteArrayInputStream> errorReportAnswer : errorReportsAnswers)
        {
            result.addErrorReportResult(
                errorReportAnswer.getT1().displayValue,
                Node.Type.SATELLITE.name(),
                deserializeErrorReports(errorReportAnswer.getT2()));
        }

        result.sort(pageComparator(sortBy, sortAsc)).slice(offset, limit);
        errorReporter.logInfo(
            "Assembled error report page; %d of %d reports",
            result.getErrorReports().size(),
            result.getTotalCount()
        );
        return result;
    }

    /**
     * Comparator over the requested sort field with stable tiebreakers, matching the ordering the
     * nodes use for their local statements so that paging neither duplicates nor skips reports.
     */
    static Comparator<ErrorReport> pageComparator(final ErrorReportSortBy sortBy, final boolean sortAsc)
    {
        Comparator<ErrorReport> cmp = sortBy.getComparator();
        if (!sortAsc)
        {
            cmp = cmp.reversed();
        }
        return cmp
            .thenComparing(Comparator.comparing(ErrorReport::getDateTime).reversed())
            .thenComparing(ErrorReport::getNodeName)
            .thenComparing(ErrorReport::getFileName);
    }

    private Flux<ErrorReportResult> fetchPageTexts(final ErrorReportResult page, final Set<String> nodesToRequest)
    {
        Set<String> controllerIds = new HashSet<>();
        Set<String> satelliteIds = new HashSet<>();
        for (ErrorReport report : page.getErrorReports())
        {
            if (report.getModule() == Node.Type.CONTROLLER)
            {
                controllerIds.add(reportIdFromFileName(report.getFileName()));
            }
            else
            {
                satelliteIds.add(reportIdFromFileName(report.getFileName()));
            }
        }

        return scopeRunner
            .fluxInTransactionlessScope(
                "Collect error report texts",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                () -> satelliteIds.isEmpty() ?
                    Flux.empty() :
                    assembleRequests(nodesToRequest, true, null, null, satelliteIds, null, null)
            )
            .collectList()
            .flatMapMany(textAnswers ->
                scopeRunner.fluxInTransactionlessScope(
                    "Apply error report texts",
                    lockGuardFactory.buildDeferred(LockType.READ), // no lock needed
                    () -> Flux.just(applyPageTexts(page, controllerIds, textAnswers))
                )
            );
    }

    private ErrorReportResult applyPageTexts(
        final ErrorReportResult page,
        final Set<String> controllerIds,
        List<Tuple2<NodeName, ByteArrayInputStream>> textAnswers
    )
        throws IOException
    {
        Map<Pair<String, String>, String> texts = new HashMap<>();
        if (!controllerIds.isEmpty())
        {
            putTexts(texts, errorReporter.listReports(true, null, null, controllerIds, null, null).getErrorReports());
        }
        for (Tuple2<NodeName, ByteArrayInputStream> textAnswer : textAnswers)
        {
            putTexts(texts, deserializeErrorReports(textAnswer.getT2()).getErrorReports());
        }

        for (ErrorReport report : page.getErrorReports())
        {
            @Nullable String text = texts.get(new Pair<>(report.getNodeName(), report.getFileName()));
            if (text != null)
            {
                report.setText(text);
            }
        }
        return page;
    }

    private static void putTexts(Map<Pair<String, String>, String> texts, List<ErrorReport> reports)
    {
        for (ErrorReport report : reports)
        {
            report.getText().ifPresent(text -> texts.put(new Pair<>(report.getNodeName(), report.getFileName()), text));
        }
    }

    private static String reportIdFromFileName(String fileName)
    {
        String id = fileName;
        if (id.startsWith(StdErrorReporter.RPT_PREFIX))
        {
            id = id.substring(StdErrorReporter.RPT_PREFIX.length());
        }
        if (id.endsWith(StdErrorReporter.RPT_SUFFIX))
        {
            id = id.substring(0, id.length() - StdErrorReporter.RPT_SUFFIX.length());
        }
        return id;
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(
        Set<String> nodesToRequest,
        boolean withContent,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset)
    {
        return assembleRequests(nodesToRequest, withContent, since, to, ids, limit, offset, null, null);
    }

    private Flux<Tuple2<NodeName, ByteArrayInputStream>> assembleRequests(
        Set<String> nodesToRequest,
        boolean withContent,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset,
        @Nullable final ErrorReportSortBy sortBy,
        @Nullable final Boolean sortAsc)
    {
        Stream<Node> nodeStream = nodeRepository.getMapForView().values().stream()
            .filter(node -> nodesToRequest.isEmpty() ||
                nodesToRequest.stream().anyMatch(node.getName().getDisplayName()::equalsIgnoreCase));

        List<Tuple2<NodeName, Flux<ByteArrayInputStream>>> nameAndRequests = nodeStream
            .map(node ->
                Tuples.of(
                    node.getName(),
                    prepareErrRequestApi(node, withContent, since, to, ids, limit, offset, sortBy, sortAsc)))
            .collect(Collectors.toList());

        return Flux
            .fromIterable(nameAndRequests)
            .flatMap(nameAndRequest -> nameAndRequest.getT2()
                .map(byteStream -> Tuples.of(nameAndRequest.getT1(), byteStream)));
    }

    private Flux<ByteArrayInputStream> prepareErrRequestApi(
        final Node node,
        boolean withContent,
        @Nullable final Instant since,
        @Nullable final Instant to,
        final Set<String> ids,
        @Nullable final Long limit,
        @Nullable final Long offset,
        @Nullable final ErrorReportSortBy sortBy,
        @Nullable final Boolean sortAsc)
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> fluxReturn = Flux.empty();
        if (peer != null)
        {
            byte[] msg = stltComSerializer.headerlessBuilder()
                .requestErrorReports(new HashSet<>(), withContent, since, to, ids, limit, offset, sortBy, sortAsc)
                .build();
            fluxReturn = peer.apiCall(ApiConsts.API_REQ_ERROR_REPORTS, msg)
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        return fluxReturn;
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        peer = node.getPeer();
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
        @Nullable final Instant since,
        @Nullable final Instant to,
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
                Instant.ofEpochMilli(errorReport.getErrorTime()),
                errorReport.getText())
            );
        }

        return new ErrorReportResult(msgErrorReport.getTotalCount(), errorReports);
    }
}
