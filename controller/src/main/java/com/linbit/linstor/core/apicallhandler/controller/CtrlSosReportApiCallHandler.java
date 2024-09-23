package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ChildProcessTimeoutException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.extproc.ChildProcessHandler;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.extproc.OutputProxy.EOFEvent;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.SosReportType;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.RequestFilePojo;
import com.linbit.linstor.api.rest.v1.serializer.Json;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.api.rest.v1.serializer.JsonSpaceTracking;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.core.cfg.LinstorConfig;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.proto.responses.FileOuterClass.File;
import com.linbit.linstor.proto.responses.MsgSosReportFilesReplyOuterClass.MsgSosReportFilesReply;
import com.linbit.linstor.proto.responses.MsgSosReportListReplyOuterClass.FileInfo;
import com.linbit.linstor.proto.responses.MsgSosReportListReplyOuterClass.MsgSosReportListReply;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.spacetracking.SpaceTrackingService;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.FileUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.CommandExec;
import com.linbit.utils.FileCollector;
import com.linbit.utils.StringUtils;
import com.linbit.utils.TimeUtils;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlSosReportApiCallHandler
{
    private static final String QUERY_FILE = "query-params";

    private static final int DFLT_DEQUE_CAPACITY = 10;

    private static final String SOS_PREFIX = "sos_";
    private static final String SOS_SUFFIX = ".tar.gz";

    /** 10MiB */
    private static final long MAX_PACKET_SIZE = 10 * (1 << 20);
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository rscDfnRepo;
    private final ObjectMapper objectMapper;
    private final CtrlApiCallHandler ctrlApiCallHandler;
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;
    private final Provider<SpaceTrackingService> spaceTrackingServiceProvider;
    private final CtrlStltSerializer stltComSerializer;
    private final ErrorReporter errorReporter;
    private final ExtCmdFactory extCmdFactory;
    private final CtrlConfig ctrlCfg;
    private final DbEngine db;

    @Inject
    public CtrlSosReportApiCallHandler(
        ErrorReporter errorReporterRef,
        @PeerContext
        Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository rscDfnRepoRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef,
        CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef,
        CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef,
        Provider<SpaceTrackingService> spaceTrackingServiceProviderRef,
        CtrlStltSerializer clientComSerializerRef,
        ExtCmdFactory extCmdFactoryRef,
        CtrlConfig ctrlCfgRef,
        DbEngine dbRef
    )
    {
        errorReporter = errorReporterRef;
        peerAccCtx = peerAccCtxRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        nodeRepository = nodeRepositoryRef;
        rscDfnRepo = rscDfnRepoRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
        spaceTrackingServiceProvider = spaceTrackingServiceProviderRef;
        stltComSerializer = clientComSerializerRef;
        extCmdFactory = extCmdFactoryRef;
        ctrlCfg = ctrlCfgRef;
        db = dbRef;

        objectMapper = new ObjectMapper();
    }

    /**
     * Overall workflow is as follows:
     * <p>
     * First, the controller sends out requests to all satellite for a file-list (name, size and length; no content).
     * We simply assume that this file-list is per satellite smaller than 16MB.
     * </p><p>
     * Next, in order to not exceed that 16MB limit, the controller requests the first files per list that sum of sizes
     * do not exceed 10MB.
     * The rest of the 16MB is a (hopefully more than generous) buffer for the overhead from Linstor-protocol and
     * protobuf, and others.
     * </p><p>
     * The responses to those requests are stored on the controller directly in the destination files.
     *</p><p>
     * Once all satellites finished the responses, the controller tar.gz's the collected files and sends a
     * cleanup-message to the satellites
     * (which deletes their temporary sos-directories).
     * </p><p>
     * The resulting String in the flux is the name of the SOS-report.
     * </p>
     */
    public Flux<String> getSosReport(
        Set<String> nodes,
        Set<String> rscs,
        Set<String> excludeNodes,
        LocalDateTime since,
        boolean includeCtrl,
        String queryParams
    )
    {
        var logContextMap = MDC.getCopyOfContextMap();
        Flux<String> ret;
        try
        {
            final Path tmpDir = Files.createTempDirectory("sos");
            String sosReportName = SOS_PREFIX + TimeUtils.DTF_NO_SPACE.format(ZonedDateTime.now(ZoneOffset.UTC));
            makeDir(tmpDir.resolve(sosReportName));

            ret = scopeRunner.fluxInTransactionlessScope(
                "Send SOS report queries",
                lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                () -> sendRequests(nodes, rscs, excludeNodes, tmpDir, sosReportName, since),
                logContextMap
            ).flatMap(
                sosFileList -> scopeRunner.fluxInTransactionalScope(
                    "Receiving SOS FileList",
                    lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP),
                    () -> handleSosFileList(tmpDir, sosReportName, sosFileList),
                    logContextMap
                )
            ).transform(
                // until now we were working with Flux<ByteArrayInputStream>s (proto messages from the satellites), but
                // now we need to prepare for the answer to the satellite, which expects a single string of the sos
                // report name
                ignore -> ignore.thenMany(Flux.<String>empty())
            ).concatWith(gatherControllerJsonInfo(tmpDir, sosReportName)
            ).concatWith(
                scopeRunner.fluxInTransactionalScope(
                    "Finishing SOS report",
                    lockGuardFactory.buildDeferred(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
                    () -> finishReport(
                        nodes, rscs, excludeNodes, tmpDir, sosReportName, since, includeCtrl, queryParams
                    ),
                    logContextMap
                )
            );
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
            ret = Flux.error(exc);
        }
        return ret;
    }

    /**
     * Sends the initial request of file-lists to all given satellites.
     * Next flux: process answers
     */
    private Flux<ByteArrayInputStream> sendRequests(
        Set<String> nodes,
        Set<String> rscs,
        Set<String> excludeNodes,
        Path tmpDir,
        String sosReportName,
        LocalDateTime since
    )
        throws AccessDeniedException
    {
        Stream<Node> nodeStream = getNodeStreamForSosReport(nodes, rscs, excludeNodes);

        List<Flux<ByteArrayInputStream>> namesAndRequests = nodeStream
            .map(node -> prepareSosRequestApi(node, tmpDir, sosReportName, since))
            .collect(Collectors.toList());

        return Flux.fromIterable(namesAndRequests).flatMap(Function.identity());
    }

    /**
     * If the peer cannot be found from the given node, return a <code>Flux.empty()</code>. Otherwise return the Flux
     * from <code>peer.apiCall(...)</code>
     */
    private Flux<ByteArrayInputStream> prepareSosRequestApi(
        Node node,
        Path tmpDir,
        String sosReportName,
        LocalDateTime since
    )
    {
        Peer peer = getPeer(node);
        Flux<ByteArrayInputStream> fluxReturn = Flux.empty();
        if (peer != null)
        {
            // already create the extTools file here, because you need the peer
            createExtToolsInfo(tmpDir.resolve(sosReportName + "/" + node.getName().displayValue), peer);
            byte[] msg = stltComSerializer.headerlessBuilder().requestSosReport(sosReportName, since).build();
            fluxReturn = peer.apiCall(InternalApiConsts.API_REQ_SOS_REPORT_FILE_LIST, msg)
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        return fluxReturn;
    }

    private Peer getPeer(Node node)
    {
        Peer peer;
        try
        {
            peer = node.getPeer(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access peer for node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return peer;
    }

    /**
     * Called when the satellite responses their file list (without content).
     * The ProtoFiles are converted into Pojos, empty files (0 bytes) are already created,
     * and <code>requestNextBatch</code> is called.
     */
    private Flux<ByteArrayInputStream> handleSosFileList(
        Path tmpDirRef,
        String sosReportName,
        ByteArrayInputStream sosFileListInputStreamRef
    )
    {
        MsgSosReportListReply msgSosReportListReply;
        Peer peer;
        try
        {
            msgSosReportListReply = MsgSosReportListReply.parseDelimitedFrom(sosFileListInputStreamRef);
            peer = getPeer(nodeRepository.get(peerAccCtx.get(), new NodeName(msgSosReportListReply.getNodeName())));
        }
        catch (IOException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An IOException occurred while parsing SosFileList"
                ),
                exc
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(accDeniedExc, "accessing node", ApiConsts.FAIL_ACC_DENIED_NODE);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }

        Flux<ByteArrayInputStream> flux;
        if (peer == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Path sosDir = tmpDirRef.resolve(sosReportName + "/" + msgSosReportListReply.getNodeName());
            String sosReportDirOnStltStr = LinStor.SOS_REPORTS_DIR.resolve(sosReportName).toString();
            String sosDirStr = sosDir.toString();
            LinkedList<RequestFilePojo> filesToRequest = new LinkedList<>();
            for (FileInfo fileInfo : msgSosReportListReply.getFilesList())
            {
                if (fileInfo.getSize() == 0)
                {
                    // do not request files with 0 bytes. instead, just create the corresponding file right now

                    String fileName = fileInfo.getName();
                    if (fileName.startsWith(sosReportDirOnStltStr))
                    {
                        // move all '$stlt/var/lib/linstor.d/sos-report/$currentSosName/*' files to '$stlt/*'.
                        fileName = fileName.substring(sosReportDirOnStltStr.length());
                    }
                    errorReporter.logTrace(
                        "Not requesting %s for file %s as its size is 0 bytes. File %s created.",
                        peer.getNode().getName().displayValue,
                        fileInfo.getName(),
                        fileName
                    );

                    append(
                        concatPaths(sosDirStr, fileName),
                        new byte[0],
                        fileInfo.getTime()
                    );
                }
                else
                {
                    filesToRequest.add(new RequestFilePojo(fileInfo.getName(), 0, fileInfo.getSize()));
                }
            }
            if (!msgSosReportListReply.getErrorMessage().isEmpty())
            {
                append(
                    sosDir.resolve("sos.err"),
                    (msgSosReportListReply.getErrorMessage() + "\n").getBytes(),
                    System.currentTimeMillis()
                );
            }
            flux = requestNextBatch(tmpDirRef, sosReportName, peer, filesToRequest);
        }

        return flux;
    }

    /**
     * This method takes the original fileList and processes all elements. A fully processed entry is deleted from the
     * list. An element is fully processed if all of its bytes can be requested from the satellite. A file needs to be
     * split into multiple requests if the current request's size exceeds 10MiB. In that case, the partially requested
     * file is re-added to the list (at beginning, at index 0) with modified offset for the next processing.
     * The prepared batch is requested from the satellite and processed in the method <code>handleReceivedFiles</code>.
     * If the fileList is empty (i.e. the last batch was requested and processed), this method returns an empty Flux
     * finishing the Flux-loop.
     */
    private Flux<ByteArrayInputStream> requestNextBatch(
        Path tmpDirRef,
        String sosReportNameRef,
        Peer stltPeerRef,
        LinkedList<RequestFilePojo> filesToRequestRef
    )
    {
        long sumFileSizes = 0;
        ArrayList<RequestFilePojo> nextBatchToRequest = new ArrayList<>();
        while (!filesToRequestRef.isEmpty())
        {
            RequestFilePojo next = filesToRequestRef.removeFirst();
            long nextSize = next.length - next.offset;
            if (nextSize + sumFileSizes <= MAX_PACKET_SIZE)
            {
                // the request fits, but it might be the last part of an split request.
                RequestFilePojo lastPartialRequestFile = new RequestFilePojo(next.name, next.offset, nextSize);
                nextBatchToRequest.add(lastPartialRequestFile);
                sumFileSizes += nextSize;
                if (lastPartialRequestFile.offset == 0)
                {
                    errorReporter.logTrace(
                        "Requesting %s for file %s (total size: %d)",
                        stltPeerRef.getNode().getName().displayValue,
                        lastPartialRequestFile.name,
                        lastPartialRequestFile.length
                    );
                }
                else
                {
                    errorReporter.logTrace(
                        "Requesting %s for last part of file %s (offset: %d, length: %d, total size: %d)",
                        stltPeerRef.getNode().getName().displayValue,
                        lastPartialRequestFile.name,
                        lastPartialRequestFile.offset,
                        lastPartialRequestFile.length,
                        next.length
                    );
                }
            }
            else
            {
                long partSize = MAX_PACKET_SIZE - sumFileSizes;

                RequestFilePojo partialRequestFile = new RequestFilePojo(next.name, next.offset, partSize);
                nextBatchToRequest.add(partialRequestFile);
                errorReporter.logTrace(
                    "Requesting %s for file %s (offset: %d, length: %d, total size: %d)",
                    stltPeerRef.getNode().getName().displayValue,
                    partialRequestFile.name,
                    partialRequestFile.offset,
                    partialRequestFile.length,
                    next.length
                );

                // safe the partially requested file for next iteration
                RequestFilePojo nextContinueFrom = new RequestFilePojo(next.name, next.offset + partSize, next.length);
                filesToRequestRef.addFirst(nextContinueFrom);
                break;
            }
        }

        Flux<ByteArrayInputStream> ret;
        if (!nextBatchToRequest.isEmpty())
        {
            ret = stltPeerRef.apiCall(
                InternalApiConsts.API_REQ_SOS_REPORT_FILES,
                stltComSerializer.headerlessBuilder()
                    .requestSosReportFiles(
                        sosReportNameRef,
                        nextBatchToRequest
                    )
                    .build()
            ).flatMap(
                response -> handleReceivedFiles(tmpDirRef, sosReportNameRef, stltPeerRef, filesToRequestRef, response)
            )
                .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty());
        }
        else
        {
            ret = Flux.empty(); // we are done with requests to this satellite
        }
        return ret;
    }

    /**
     * Processes the requested batch (writes the content to the corresponding files) and calls the previous method
     * (<code>requestNextBatch</code>) for the next batch.
     * <p>
     * If a processed file starts with /var/lib/linstor.d/sos-report/$currentSosName/, instead of creating a
     * satellite-local directory with that name, we simplycreate the file into the satellite-root directory. In other
     * words all '$stlt/var/lib/linstor.d/sos-report/$currentSosName/*' files are going to land in '$stlt/*'.
     * </p>
     */
    private Flux<ByteArrayInputStream> handleReceivedFiles(
        Path tmpDirRef,
        String sosReportNameRef,
        Peer stltPeerRef,
        LinkedList<RequestFilePojo> filesToRequestRef,
        ByteArrayInputStream responseRef
    )
    {
        MsgSosReportFilesReply protoFilesReply;
        try
        {
            protoFilesReply = MsgSosReportFilesReply.parseDelimitedFrom(responseRef);
        }
        catch (IOException exc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "An IOException occurred while parsing SosFileReply"
                ),
                exc
            );
        }

        String nodeName = protoFilesReply.getNodeName();
        Path sosDir = tmpDirRef.resolve(sosReportNameRef + "/" + nodeName);
        String sosDirStr = sosDir.toString();
        String sosReportDirOnStltStr = LinStor.SOS_REPORTS_DIR.resolve(sosReportNameRef).toString();
        for (File file : protoFilesReply.getFilesList())
        {
            String fileName = file.getFileName();
            if (fileName.startsWith(sosReportDirOnStltStr))
            {
                // move all '$stlt/var/lib/linstor.d/sos-report/$currentSosName/*' files to '$stlt/*'.
                fileName = fileName.substring(sosReportDirOnStltStr.length());
            }
            append(
                concatPaths(sosDirStr, fileName),
                file.getContent().toByteArray(),
                file.getTime()
            );
        }
        return requestNextBatch(tmpDirRef, sosReportNameRef, stltPeerRef, filesToRequestRef);
    }

    /**
     * After all files are processed, this method stores the controller-related information (if the
     * filtered node-list contains "Controller" ignoring case), tars the SOS report which also deletes the temporary
     * directory on the controller, sends out a cleanup message to all participating satellites (to delete their
     * temporary SOS-directory) and returns a Flux containing the sos report name.
     */
    private Flux<String> finishReport(
        Set<String> nodes,
        Set<String> rscs,
        Set<String> excludeNodes,
        Path tmpDir,
        String sosReportName,
        LocalDateTime since,
        boolean includeCtrl,
        String queryParams
    )
        throws IOException, AccessDeniedException, ChildProcessTimeoutException, ExtCmdFailedException
    {
        String fileName = errorReporter.getLogDirectory() + "/" + sosReportName + SOS_SUFFIX;

        Stream<Node> nodeStream = getNodeStreamForSosReport(nodes, rscs, excludeNodes);
        List<Node> nodeList = nodeStream.collect(Collectors.toList());
        List<String> namesForTar = nodeList.stream().map(node -> sosReportName + "/" + node.getName().displayValue)
            .collect(Collectors.toList());
        for (String name : namesForTar)
        {
            try
            {
                makeDir(tmpDir.resolve(name));
            }
            catch (IOException exc)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_UNKNOWN_ERROR,
                        "Failed to create directory: " + tmpDir.resolve(name)
                    ),
                    exc
                );
            }
        }
        if (includeCtrl)
        {
            namesForTar.add(sosReportName + "/_" + LinStor.CONTROLLER_MODULE);
            createControllerFilesInto(tmpDir, sosReportName, since);
        }
        namesForTar.add(sosReportName + "/" + QUERY_FILE);
        append(
            tmpDir.resolve(sosReportName + "/" + QUERY_FILE),
            (queryParams == null ? "<no query-params>" : queryParams).getBytes(),
            System.currentTimeMillis()
        );
        createTar(tmpDir, fileName, namesForTar);

        Flux<ApiCallRc> ret = Flux.empty();

        for (Node node : nodeList)
        {
            ret = ret.concatWith(
                node.getPeer(peerAccCtx.get()).apiCall(
                    InternalApiConsts.API_REQ_SOS_REPORT_CLEANUP,
                    stltComSerializer.headerlessBuilder()
                        .cleanupSosReport(sosReportName)
                        .build()
                ).map(
                    answer -> CtrlSatelliteUpdateCaller.deserializeApiCallRc(node.getName(), answer)
                )
                    .onErrorResume(PeerNotConnectedException.class, ignored -> Flux.empty())
                    // not sure why this is needed when we .thenMany later on... but without this, we get
                    // ClassCastExceptions as ApiCallRcImpl cannot be casted to String
                    .ignoreElements()
            );
        }

        return ret.thenMany(Flux.just(fileName));
    }

    private Flux<String> gatherControllerJsonInfo(Path tmpDir, String sosReportName)
    {
        String nodeName = "_" + LinStor.CONTROLLER_MODULE;
        Path sosDir = tmpDir.resolve(sosReportName + "/" + nodeName);

        Flux<String> fluxFromSatellites = storpoolListJson(sosDir)
            .concatWith(viewResourcesJson(sosDir));

        return fluxFromSatellites.concatWith(scopeRunner.fluxInTransactionalScope(
                "sos-json-lists-local",
                lockGuardFactory.buildDeferred(
                    LockType.READ, LockObj.NODES_MAP, LockObj.RSC_GRP_MAP),
                () -> {
                    nodeListJson(sosDir);
                    resourceGroupListJson(sosDir);
                    spaceReportingQuery(sosDir);
                    return Flux.empty();
                }, MDC.getCopyOfContextMap()));
    }

    private void appendJSON(Path file, Object content)
    {
        try
        {
            append(
                file,
                objectMapper.writeValueAsBytes(content),
                System.currentTimeMillis());
        }
        catch (JsonProcessingException jexc)
        {
            errorReporter.reportError(jexc);
        }
    }

    private void nodeListJson(Path sosDir)
    {
        List<JsonGenTypes.Node> nodeDataList = ctrlApiCallHandler.listNodes(
                Collections.emptyList(),
                Collections.emptyList()).stream()
            .map(Json::apiToNode)
            .collect(Collectors.toList());

        appendJSON(sosDir.resolve("node-list.json"), nodeDataList);
    }

    private Flux<String> storpoolListJson(Path sosDir)
    {
        Flux<List<StorPoolApi>> flux = ctrlStorPoolListApiCallHandler
            .listStorPools(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), true);

        var logContextMap = MDC.getCopyOfContextMap();
        return flux.flatMap(storPoolApis -> {
            MDC.setContextMap(logContextMap);
            List<JsonGenTypes.StoragePool> storPoolDataList = storPoolApis.stream()
                .map(Json::storPoolApiToStoragePool)
                .collect(Collectors.toList());

            appendJSON(sosDir.resolve("stor-pool-list.json"), storPoolDataList);

            return Flux.empty();
        });
    }

    private void resourceGroupListJson(Path sosDir)
    {
        var resGrpList = ctrlApiCallHandler
                .listResourceGroups(Collections.emptyList(), Collections.emptyList()).stream()
            .map(Json::apiToResourceGroup)
            .collect(Collectors.toList());

        appendJSON(sosDir.resolve("resource-group-list.json"), resGrpList);
    }

    private Flux<String> viewResourcesJson(Path sosDir)
    {
        Flux<ResourceList> flux = ctrlVlmListApiCallHandler.listVlms(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        var logContextMap = MDC.getCopyOfContextMap();
        return flux.flatMap(resourceList -> {
            MDC.setContextMap(logContextMap);
            final List<JsonGenTypes.Resource> rscs = resourceList.getResources().stream()
                .map(rscApi -> Json.apiToResourceWithVolumes(rscApi, resourceList.getSatelliteStates(), true))
                .collect(Collectors.toList());

            appendJSON(sosDir.resolve("view-resources.json"), rscs);

            return Flux.empty();
        });
    }

    private void spaceReportingQuery(Path sosDir)
    {
        JsonSpaceTracking.SpaceReport jsonReportText = new JsonSpaceTracking.SpaceReport();
        try
        {
            SpaceTrackingService stSvc = spaceTrackingServiceProvider.get();
            if (stSvc != null)
            {
                jsonReportText.reportText = stSvc.querySpaceReport(null);
            }
            else
            {
                jsonReportText.reportText = "The SpaceTracking service is not installed.";
            }
        }
        catch (DatabaseException | NoSuchAlgorithmException | DigestException exc)
        {
            jsonReportText.reportText = exc.getMessage();
            errorReporter.reportError(exc);
        }

        appendJSON(sosDir.resolve("space-reporting-query.json"), jsonReportText);
    }

    /**
     * Creates a file with all supported and unsupported external tools of the given satellite in the given directory.
     */
    private void createExtToolsInfo(Path dirPath, Peer peer)
    {
        try
        {
            makeDir(dirPath);
            StringBuilder sb = new StringBuilder();
            ExtTools[] extTools = ExtTools.values();
            for (ExtTools tool : extTools)
            {
                ExtToolsInfo info = peer.getExtToolsManager().getExtToolInfo(tool);
                sb.append(tool.name()).append("\n");
                if (info.isSupported())
                {
                    sb.append("\tSupported\n");
                    sb.append("\tVersion: ")
                        .append(info.getVersionMajor())
                        .append(".")
                        .append(info.getVersionMinor())
                        .append(".")
                        .append(info.getVersionPatch())
                        .append("\n");
                }
                else
                {
                    sb.append("\tNot Supported\n");
                    sb.append("\tReason:\n");
                    for (String reason : info.getNotSupportedReasons())
                    {
                        sb.append("\t\t").append(reason).append("\n");
                    }
                }
            }
            Files.write(dirPath.resolve("extTools"), sb.toString().getBytes());
        }
        catch (IOException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    /**
     * Collects all controller-based information (see list below) into the given <code>$tmpDir/_Controller</code>.
     * <p>
     * Collected files/info:
     * <table>
     * <style>table tr td { padding-right: 10px; }</style>
     * <tr><th>Filename</th><th>Content</th></tr>
     * <tr><td>linstor.toml</td><td>'cp -p $actual_linstor.toml $sosDir'</td></tr>
     * <tr><td>journalctl</td><td>'journal -u linstor-controller --since $since'</td></tr>
     * <tr><td>ip-a</td><td>'ip a'</td></tr>
     * <tr><td>log-syslog</td><td>'cp -p /var/log/syslog $sosDir/log-syslog'</td></tr>
     * <tr><td>log-kern.log</td><td>'cp -p /var/log/kern.log $sosDir/log-kern.log'</td></tr>
     * <tr><td>log-messages</td><td>'cp -p /var/log/messages $sosdir/log-messages'</td></tr>
     * <tr><td>release</td><td>'cat /etc/redhat-release /etc/lsb-release /etc/os-release'</td></tr>
     * </table>
     * </p>
     */
    private void createControllerFilesInto(Path tmpDir, String sosReportName, LocalDateTime since) throws IOException
    {
        long nowMillis = System.currentTimeMillis();
        LocalDateTime now = TimeUtils.millisToDate(nowMillis);

        String nodeName = "_" + LinStor.CONTROLLER_MODULE;
        Path sosDir = tmpDir.resolve(sosReportName + "/" + nodeName);
        String infoContent = LinStor.linstorInfo() + "\n\nuname -a:           " + LinStor.getUname("-a");
        append(sosDir.resolve("linstorInfo"), infoContent.getBytes(), nowMillis);
        String timeContent = "Local Time: " + TimeUtils.DTF_NO_SPACE.format(now) + "\nUTC Time:   " +
            TimeUtils.DTF_NO_SPACE
            .format(ZonedDateTime.of(now, ZoneOffset.UTC));
        append(sosDir.resolve("timeInfo"), timeContent.getBytes(), nowMillis);
        getDbDump(sosDir);

        String tomlPath = ctrlCfg.getConfigDir() + LinstorConfig.LINSTOR_CTRL_CONFIG;
        CommandHelper[] commands = new CommandHelper[]
        {
            new CommandHelper(
                sosDir.resolve(LinstorConfig.LINSTOR_CTRL_CONFIG),
                new String[]
                {
                    "cp", "-p", tomlPath, sosDir.toString()
                }
            ),
            new CommandHelper(
                sosDir.resolve("journalctl"),
                new String[]
                {
                    "journalctl", "-u", "linstor-controller", "--since", TimeUtils.JOURNALCTL_DF.format(since)
                }
            ),
            new CommandHelper(
                sosDir.resolve("journalctl-kernel-log"),
                new String[]
                    {
                        "journalctl", "--dmesg", "-b", "all", "--since", TimeUtils.JOURNALCTL_DF.format(since)
                    }
                ),
            new CommandHelper(
                sosDir.resolve("ip-a"),
                new String[]
                {
                    "ip", "a"
                }
            ),
            new CommandHelper(
                sosDir.resolve("log-syslog"),
                new String[]
                {
                    "cp", "-p", "/var/log/syslog", sosDir + "/log-syslog"
                }
            ),
            new CommandHelper(
                sosDir.resolve("log-kern.log"),
                new String[]
                {
                    "cp", "-p", "/var/log/kern.log", sosDir + "/log-kern.log"
                }
            ),
            new CommandHelper(
                sosDir.resolve("log-messages"),
                new String[]
                {
                    "cp", "-p", "/var/log/messages", sosDir + "/log-messages"
                }
            ),
            new CommandHelper(
                sosDir.resolve("release"),
                new String[]
                {
                    "cat", "/etc/redhat-release", "/etc/lsb-release", "/etc/os-release"
                }
            ),
        };
        for (CommandHelper cmd : commands)
        {
            try
            {
                CommandExec.executeCmd(cmd.cmd, cmd.file.toFile(), new java.io.File(cmd.file + ".err"), nowMillis);
            }
            catch (IOException | InterruptedException exc)
            {
                byte[] exceptionData = CommandExec.exceptionToString(exc).getBytes();
                try
                {
                    Path fileNameIoExc = sosDir.resolve(cmd.file.getFileName() + "io_exc");
                    Files.write(fileNameIoExc, exceptionData);
                }
                catch (IOException exc1)
                {
                    errorReporter.reportError(exc1);
                }
            }
        }

        FileCollector collector = new FileCollector();
        Files.walkFileTree(errorReporter.getLogDirectory(), collector);
        Set<SosReportType> errorReports = collector.getFiles();
        if (!errorReports.isEmpty())
        {
            makeDir(sosDir.resolve("logs/"));
        }

        for (SosReportType err : errorReports)
        {
            Path erroReportPath = sosDir.resolve("logs/" + Paths.get(err.getFileName()).getFileName());
            makeFileFromCmdNoFailed(
                addExtension(erroReportPath, ".out"),
                nowMillis,
                "cp", "-p",
                errorReporter.getLogDirectory().resolve(err.getFileName()).toString(),
                erroReportPath.toString()
            );
        }
    }

    private void makeDir(Path dirPath) throws IOException
    {
        if (!Files.exists(dirPath))
        {
            Files.createDirectory(dirPath);
        }
    }

    /**
     * Runs the given command and stores the stdOut in the given file.
     * The given file's lastModified date is set in the end.
     * <p>
     * If a problem occurs, a new file with the additional ".err" is created with the content of the stdErr.
     * Another such special file is created for ".ioexc" (containing the stacktrace).
     * </p><p>
     * If there is neither stdOut nor stdErr (and no exception) no files will be created at all (in case of a 'cp ...'
     * command for example)
     * </p>
     */
    private void makeFileFromCmdNoFailed(Path filePath, long timestamp, String... command)
    {
        LinkedBlockingDeque<Event> deque = new LinkedBlockingDeque<>(DFLT_DEQUE_CAPACITY);
        DaemonHandler dh = new DaemonHandler(deque, command);

        Path outFile = filePath;
        Path errFile = addExtension(filePath, ".err");

        boolean running;
        try
        {
            dh.startUndelimited();
            running = true;
        }
        catch (IOException exc)
        {
            running = false;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(baos);
            exc.printStackTrace(pw);
            pw.flush();
            byte[] data = baos.toByteArray();

            append(addExtension(filePath, ".ioexc"), data, timestamp, command);
        }

        while (running)
        {
            Event event;

            byte[] data = null;
            Path file = null;
            try
            {
                event = deque.take();

                if (event instanceof StdOutEvent)
                {
                    StdOutEvent stdOutEvent = (StdOutEvent) event;
                    data = stdOutEvent.data;
                    file = outFile;
                }
                else if (event instanceof StdErrEvent)
                {
                    StdErrEvent stdErrEvent = (StdErrEvent) event;
                    data = stdErrEvent.data;
                    file = errFile;
                }
                else if (event instanceof EOFEvent)
                {
                    running = false;
                }
                else if (event instanceof ExceptionEvent)
                {
                    running = false;

                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    PrintWriter pw = new PrintWriter(baos);
                    ((ExceptionEvent) event).exc.printStackTrace(pw);
                    pw.flush();
                    data = baos.toByteArray();

                    file = addExtension(filePath, ".exc");
                }
                else
                {
                    errorReporter.logError(
                        "Unknown event type during SOS report: %s while processing command %s",
                        event.getClass().getCanonicalName(),
                        StringUtils.joinShellQuote(command)
                    );
                }
            }
            catch (InterruptedException exc)
            {
                running = false;
                data = "Interrupted".getBytes();
                file = outFile;

                Thread.currentThread().interrupt();
            }
            if (file != null && data != null && data.length > 0)
            {
                append(file, data, timestamp, command);
            }
        }
    }

    private Path addExtension(Path pathRef, String extensionRef)
    {
        return pathRef.getParent().resolve(pathRef.getFileName() + extensionRef);
    }

    /**
     * Concatenates two paths with each other.
     * This is intentionally NOT done by using first.resolve(second) since if second is an absolute path the result of
     * the .resolve call is just plain the parameter (in this case $second).
     */
    private Path concatPaths(String first, String second)
    {
        return Paths.get(first, second);
    }

    /**
     * Appends the given data to the given path and setting the lastModifiedTime.
     * The given command is NOT executed, just needed for possible error reports.
     */
    private void append(Path file, byte[] data, long timestampRef, String... command)
    {
        try
        {
            if (!Files.exists(file.getParent()))
            {
                Files.createDirectories(file.getParent());
            }
            Files.write(file, data, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            errorReporter.logTrace("Written %d bytes to file %s", data.length, file.toString());
            Files.setAttribute(
                file,
                "lastModifiedTime",
                FileTime.fromMillis(timestampRef)
            );
        }
        catch (IOException exc)
        {
            if (command != null && command.length > 0)
            {
                errorReporter.logError(
                    "IOException occured while writing to %s from the command '%s'",
                    file.toString(),
                    StringUtils.joinShellQuote(command)
                );
            }
            else
            {
                errorReporter.logError(
                    "IOException occured while writing to %s",
                    file.toString()
                );
            }
            errorReporter.reportError(exc);
        }

    }

    /**
     * Writes a database dump to $dirPath/dbDump
     */
    private void getDbDump(Path dirPath) throws IOException
    {
        try
        {
            Path dbDump = dirPath.resolve("dbDump");
            Files.write(dbDump, db.getDbDump().getBytes());
        }
        catch (DatabaseException exc)
        {
            String reportName = errorReporter.reportError(exc);
            append(
                dirPath.resolve("dbDump.failed"),
                ("ErrorReport-" + reportName).getBytes(),
                System.currentTimeMillis()
            );
        }
    }

    /**
     * Creates a $fileName.tar.gz file from the given source directory using the given directories relative to
     * '$source/tmp'.
     * The $source path is 'rm -rf'ed in the end.
     *
     * @param source The directory in which the tar command should operate.
     * @param fileName The resulting filename of the tar command.
     * @param directories The list of directories to include in the tar.
     *
     * @throws IOException Any IO error from the tar command (i.e. no space left).
     * @throws ExtCmdFailedException If the tar command returns a non-zero exit code.
     * @throws ChildProcessTimeoutException If the tar command does not return after 4 minutes.
     */
    private void createTar(@Nonnull Path source, @Nonnull String fileName, @Nonnull List<String> directories)
        throws IOException, ExtCmdFailedException, ChildProcessTimeoutException
    {
        ExtCmd extCommand = extCmdFactory.create();
        extCommand.setTimeout(ChildProcessHandler.TimeoutType.WAIT, 4 * 60 * 1000);
        List<String> cmd = new ArrayList<>();
        cmd.add("tar");
        cmd.add("-C");
        cmd.add(source.toString());
        cmd.add("-czf"); // no -v to prevent "Data buffer size exceeded" exception
        cmd.add(fileName);
        cmd.addAll(directories);
        String[] command = cmd.toArray(new String[0]);

        OutputData output = extCommand.exec(command);
        if (output.exitCode != 0)
        {
            throw new ExtCmdFailedException(command, output);
        }

        FileUtils.deleteDirectoryWithContent(source, errorReporter);
    }

    private Stream<Node> getNodeStreamForSosReport(Set<String> nodes, Set<String> rscs, Set<String> excludeNodes)
        throws AccessDeniedException
    {
        Set<Node> rscNodes = new HashSet<>();
        Set<String> copyRscs = rscs.stream().map(String::toLowerCase).collect(Collectors.toSet());
        if (!copyRscs.isEmpty())
        {
            for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView(peerAccCtx.get()).values())
            {
                if (copyRscs.contains(rscDfn.getName().getDisplayName().toLowerCase()))
                {
                    rscDfn.streamResource(peerAccCtx.get()).forEach(rsc -> rscNodes.add(rsc.getNode()));
                }
            }
        }

        Set<String> copyNodes = nodes.stream().map(String::toLowerCase).collect(Collectors.toSet());
        Stream<Node> nodeStream = nodeRepository.getMapForView(peerAccCtx.get()).values().stream();
        if (!copyNodes.isEmpty() || !rscNodes.isEmpty())
        {
            nodeStream = nodeStream.filter(
                node -> copyNodes.contains(node.getName().getDisplayName().toLowerCase()) || rscNodes.contains(node)
            );
        }
        Set<String> copyExclude = excludeNodes.stream().map(String::toLowerCase).collect(Collectors.toSet());
        if (!copyExclude.isEmpty())
        {
            nodeStream = nodeStream
                .filter(node -> !copyExclude.contains(node.getName().getDisplayName().toLowerCase()));
        }
        return nodeStream;
    }

    private static class CommandHelper
    {
        private final Path file;
        private final String[] cmd;

        private CommandHelper(
            Path fileRef,
            String[] cmdRef
        )
        {
            file = fileRef;
            cmd = cmdRef;
        }
    }
}
