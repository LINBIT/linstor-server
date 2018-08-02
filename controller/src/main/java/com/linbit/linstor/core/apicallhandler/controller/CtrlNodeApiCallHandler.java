package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterface.EncryptionType;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceDataFactory;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataControllerFactory;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.NodeRepository;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;

import static com.linbit.utils.StringUtils.firstLetterCaps;
import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlNodeApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final NodeDataControllerFactory nodeDataFactory;
    private final NetInterfaceDataFactory netInterfaceDataFactory;
    private final NodeRepository nodeRepository;
    private final CtrlClientSerializer clientComSerializer;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final SatelliteConnector satelliteConnector;
    private final ResponseConverter responseConverter;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    public class ErrorReportRequest
    {
        public int msgId;
        public LocalDateTime requestTime;
        public int msgReqId;
        public TreeSet<ErrorReport> errorReports;
        public TreeSet<String> requestNodes;

        public ErrorReportRequest(int msgIdRef, int msgReqIdRef)
        {
            msgId = msgIdRef;
            msgReqId = msgReqIdRef;
            requestTime = LocalDateTime.now();
            errorReports = new TreeSet<>();
            requestNodes = new TreeSet<>();
        }

        public int getMsgReqId()
        {
            return msgReqId;
        }

        @Override
        public String toString()
        {
            return requestTime.toString();
        }
    }

    public static Map<Pair<Peer, Integer>, ErrorReportRequest> errorReportMap = new HashMap<>();

    @Inject
    public CtrlNodeApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        NodeDataControllerFactory nodeDataFactoryRef,
        NetInterfaceDataFactory netInterfaceDataFactoryRef,
        NodeRepository nodeRepositoryRef,
        CtrlClientSerializer clientComSerializerRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        SatelliteConnector satelliteConnectorRef,
        ResponseConverter responseConverterRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        nodeDataFactory = nodeDataFactoryRef;
        netInterfaceDataFactory = netInterfaceDataFactoryRef;
        nodeRepository = nodeRepositoryRef;
        clientComSerializer = clientComSerializerRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        satelliteConnector = satelliteConnectorRef;
        responseConverter = responseConverterRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    /**
     * Attempts to create a node by the given parameters. <br />
     * <br />
     * In any case an {@link ApiCallRc} is returned. The list of {@link ApiCallRcEntry}s describe the success
     * or failure of the operation. <br />
     * <br />
     * All return codes from this method are masked with {@link ApiConsts#MASK_NODE} and
     * {@link ApiConsts#MASK_CRT}.<br />
     * <br />
     * Following return codes can be returned:
     * <ul>
     *  <li>
     *      {@link ApiConsts#FAIL_ACC_DENIED_NODE} when the current access context does have enough privileges to
     *      change any nodes at all (controller.nodesMapLockProt)
     *  </li>
     *  <li>{@link ApiConsts#FAIL_MISSING_NETCOM} when the list of network interface apis is empty</li>
     *  <li>
     *      {@link ApiConsts#FAIL_INVLD_NET_NAME} when the list of network interface apis contains an invalid
     *      {@link NetInterfaceName}
     *  </li>
     *  <li>
     *      {@link ApiConsts#FAIL_INVLD_NET_ADDR} when the list of network interface apis contains an invalid
     *      {@link LsIpAddress}
     *  </li>
     *  <li>{@link ApiConsts#FAIL_MISSING_STLT_CONN} when the list of satellite connection apis is empty</li>
     *  <li>{@link ApiConsts#FAIL_INVLD_NODE_NAME} when the {@link NodeName} is invalid</li>
     *  <li>{@link ApiConsts#FAIL_INVLD_NODE_TYPE} when the {@link NodeType} is invalid</li>
     *  <li>{@link ApiConsts#CREATED} when the node was created successfully </li>
     * </ul>
     *
     * @param nodeNameStr
     * @param nodeTypeStr
     * @param netIfs
     * @param propsMap
     * @return
     */
    public ApiCallRc createNode(
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        Map<String, String> propsMap
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeContext(
            peer.get(),
            ApiOperation.makeRegisterOperation(),
            nodeNameStr
        );

        try
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);

            NodeType type = asNodeType(nodeTypeStr);

            Node node = createNode(nodeName, type);

            ctrlPropsHelper.fillProperties(
                LinStorObject.NODE, propsMap, ctrlPropsHelper.getProps(node), ApiConsts.FAIL_ACC_DENIED_NODE);

            if (netIfs.isEmpty())
            {
                // TODO for auxiliary nodes maybe no netif required?
                reportMissingNetInterfaces(nodeNameStr);
            }
            else
            {
                Map<String, NetInterface> netIfMap = new TreeMap<>();

                for (NetInterfaceApi netIfApi : netIfs)
                {
                    TcpPortNumber port = null;
                    EncryptionType encrType = null;
                    if (netIfApi.isUsableAsSatelliteConnection())
                    {
                        port = LinstorParsingUtils.asTcpPortNumber(netIfApi.getSatelliteConnectionPort());
                        encrType = asEncryptionType(netIfApi.getSatelliteConnectionEncryptionType());
                    }

                    NetInterfaceData netIf = createNetInterface(
                        node,
                        LinstorParsingUtils.asNetInterfaceName(netIfApi.getName()),
                        LinstorParsingUtils.asLsIpAddress(netIfApi.getAddress()),
                        port,
                        encrType
                    );

                    if (netIfApi.isUsableAsSatelliteConnection() &&
                        getCurrentStltConn(node) == null
                    )
                    {
                        setCurrentStltConn(node, netIf);
                    }
                    netIfMap.put(netIfApi.getName(), netIf);
                }

                if (getCurrentStltConn(node) == null)
                {
                    responseConverter.addWithDetail(responses, context, ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_NO_STLT_CONN_DEFINED,
                        "No satellite connection defined for " + getNodeDescriptionInline(nodeNameStr)
                    ));
                }

                ctrlTransactionHelper.commit();
                nodeRepository.put(apiCtx, nodeName, node);

                responseConverter.addWithOp(responses, context,
                    ApiSuccessUtils.defaultRegisteredEntry(node.getUuid(), getNodeDescriptionInline(node)));

                if (type.equals(NodeType.SATELLITE) || type.equals(NodeType.COMBINED))
                {
                    satelliteConnector.startConnecting(node, peerAccCtx.get());
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void setCurrentStltConn(Node node, NetInterfaceData netIf)
    {
        try
        {
            node.setSatelliteConnection(peerAccCtx.get(), netIf);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "set the current satellite connection of " + getNodeDescriptionInline(node),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw new ApiSQLException(exc);
        }
    }

    private NetInterface getCurrentStltConn(Node node)
    {
        NetInterface netIf = null;
        try
        {
            netIf = node.getSatelliteConnection(peerAccCtx.get());
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "access the current satellite connection of " + getNodeDescriptionInline(node),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return netIf;
    }

    public ApiCallRc modifyNode(
        UUID nodeUuid,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeContext(
            peer.get(),
            ApiOperation.makeModifyOperation(),
            nodeNameStr
        );

        try
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
            NodeData node = ctrlApiDataLoader.loadNode(nodeName, true);
            if (nodeUuid != null && !nodeUuid.equals(node.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_NODE,
                    "UUID-check failed"
                ));
            }
            if (nodeTypeStr != null)
            {
                setNodeType(node, nodeTypeStr);
            }

            Props props = ctrlPropsHelper.getProps(node);
            ctrlPropsHelper.fillProperties(LinStorObject.NODE, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_NODE);

            Map<String, String> nodeProps = props.map();
            for (String key : deletePropKeys)
            {
                nodeProps.remove(key);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithDetail(
                responses, context, ctrlSatelliteUpdater.updateSatellites(node, true));
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                node.getUuid(), getNodeDescriptionInline(node)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    ApiCallRc deleteNode(String nodeNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeNameStr
        );

        try
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
            NodeData nodeData = ctrlApiDataLoader.loadNode(nodeName, false);
            if (nodeData == null)
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "Deletion of node '" + nodeName + "' had no effect."
                    )
                    .setCause("Node '" + nodeName + "' does not exist.")
                    .build()
                );
            }
            else
            {
                boolean success = true;
                boolean hasRsc = false;

                Set<ResourceDefinition> changedResourceDefinitions = new HashSet<>();

                for (Resource rsc : getRscStream(nodeData).collect(toList()))
                {
                    hasRsc = true;
                    markDeleted(rsc);
                    changedResourceDefinitions.add(rsc.getDefinition());
                }
                if (!hasRsc)
                {
                    // If the node has no resources, then there should not be any volumes referenced
                    // by the storage pool -- double check
                    Iterator<StorPool> storPoolIterator = getStorPoolIterator(nodeData);
                    while (storPoolIterator.hasNext())
                    {
                        StorPool storPool = storPoolIterator.next();
                        if (!hasVolumes(storPool))
                        {
                            delete(storPool);
                        }
                        else
                        {
                            success = false;
                            responseConverter.addWithDetail(responses, context, ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_EXISTS_VLM,
                                String.format(
                                    "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                                        "on this node, although the node does not reference any resources",
                                    nodeNameStr,
                                    storPool.getName().displayValue
                                )
                            ));
                        }
                    }
                }

                if (success)
                {
                    String successMessage = firstLetterCaps(getNodeDescriptionInline(nodeNameStr));
                    UUID nodeUuid = nodeData.getUuid(); // store node uuid to avoid deleted node acess
                    if (hasRsc)
                    {
                        markDeleted(nodeData);
                        successMessage += " marked for deletion.";
                    }
                    else
                    {
                        delete(nodeData);
                        successMessage += " deleted.";
                    }

                    ctrlTransactionHelper.commit();

                    if (!hasRsc)
                    {
                        nodeRepository.remove(apiCtx, nodeName);

                        Peer nodePeer = nodeData.getPeer(apiCtx);
                        if (nodePeer != null)
                        {
                            nodePeer.closeConnection();
                        }
                    }
                    else
                    {
                        for (ResourceDefinition rscDfn : changedResourceDefinitions)
                        {
                            // in case the node still has some resources deployed, we just marked
                            // those to be deleted and now we have to send the new DELETE-flags
                            // to the satellites.
                            responseConverter.addWithDetail(
                                responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));

                            // when they finished undeploying the resource(s), the corresponding
                            // "resourceDeleted" method in CtrlRscApiCallHandler will check if the
                            // node also needs to be deleted, and does so if needed.
                        }
                        responseConverter.addWithDetail(
                            responses, context, ctrlSatelliteUpdater.updateSatellites(nodeData, true));
                    }

                    responseConverter.addWithOp(responses, context, ApiCallRcImpl
                        .entryBuilder(ApiConsts.DELETED, successMessage)
                        .setDetails(firstLetterCaps(getNodeDescriptionInline(nodeNameStr)) +
                            " UUID is: " + nodeUuid.toString())
                        .build()
                    );

                    // TODO: tell satellites to remove all the corresponding resources and storPools
                    // TODO: if satellites finished, cleanup the storPools and then remove the node from DB
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    ApiCallRc lostNode(String nodeNameStr)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeNodeContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeNameStr
        );

        try
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = LinstorParsingUtils.asNodeName(nodeNameStr);
            NodeData nodeData = ctrlApiDataLoader.loadNode(nodeName, false);
            if (nodeData == null)
            {
                responseConverter.addWithDetail(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.WARN_NOT_FOUND,
                        "Deletion of node '" + nodeName + "' had no effect."
                    )
                    .setCause("Node '" + nodeName + "' does not exist.")
                    .build()
                );
            }
            else
            {
                Peer nodePeer = nodeData.getPeer(apiCtx);
                if (nodePeer != null && nodePeer.isConnected())
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_NODE_CONN,
                        String.format(
                            "Node '%s' still connected, please use the '%s' api.",
                            nodeNameStr,
                            ApiConsts.API_DEL_NODE
                        )
                    ));
                }

                // set node mark deleted for updates to other satellites
                nodeData.markDeleted(apiCtx);
                // inform other satellites that the node is gone
                responseConverter.addWithDetail(
                    responses, context, ctrlSatelliteUpdater.updateSatellites(nodeData, true));

                // gather all resources of the lost node and circumvent concurrent modification
                List<Resource> rscToDelete = getRscStream(nodeData).collect(toList());

                // delete all resources of the lost node
                rscToDelete.forEach(this::delete);

                // If the node has no resources, then there should not be any volumes referenced
                // by the storage pool -- double check and delete storage pools
                Iterator<StorPool> storPoolIterator = getStorPoolIterator(nodeData);
                while (storPoolIterator.hasNext())
                {
                    StorPool storPool = storPoolIterator.next();
                    if (!hasVolumes(storPool))
                    {
                        delete(storPool);
                    }
                    else
                    {
                        throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_EXISTS_VLM,
                            String.format(
                                "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                                    "on this node, although the node does not reference any resources",
                                nodeNameStr,
                                storPool.getName().displayValue
                            )
                        ));
                    }
                }


                String successMessage = firstLetterCaps(getNodeDescriptionInline(nodeNameStr)) + " deleted.";
                UUID nodeUuid = nodeData.getUuid(); // store node uuid to avoid deleted node access

                delete(nodeData);

                ctrlTransactionHelper.commit();

                nodeRepository.remove(apiCtx, nodeName);

                responseConverter.addWithOp(responses, context, ApiCallRcImpl
                    .entryBuilder(ApiConsts.DELETED, successMessage)
                    .setDetails(firstLetterCaps(getNodeDescriptionInline(nodeNameStr)) +
                        " UUID is: " + nodeUuid.toString())
                    .build()
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    byte[] listNodes(int msgId)
    {
        ArrayList<Node.NodeApi> nodes = new ArrayList<>();
        try
        {
            for (Node node : nodeRepository.getMapForView(peerAccCtx.get()).values())
            {
                try
                {
                    nodes.add(node.getApiData(peerAccCtx.get(), null, null));
                    // fullSyncId and updateId null, as they are not going to be serialized by
                    // .nodeList anyways
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add nodes we have not access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return clientComSerializer.builder(ApiConsts.API_LST_NODE, msgId).nodeList(nodes).build();
    }

    void listErrorReports(
        Peer client,
        int msgId,
        final Set<String> nodes,
        boolean withContent,
        final Optional<Date> since,
        final Optional<Date> to,
        final Set<String> ids
    )
    {
        Optional<Integer> reqId = errorReportMap.values().stream()
            .max(Comparator.comparingInt(ErrorReportRequest::getMsgReqId))
            .map(errReq -> errReq.msgReqId);

        final Pair<Peer, Integer> key = new Pair<>(client, msgId);
        errorReportMap.put(key, new ErrorReportRequest(msgId, reqId.orElse(1)));
        final ErrorReportRequest errReq = errorReportMap.get(key);
        if (nodes.isEmpty() || nodes.stream().anyMatch("controller"::equalsIgnoreCase)) {
            Set<ErrorReport> errorReports = StdErrorReporter.listReports(
                "Controller",
                errorReporter.getLogDirectory(),
                withContent,
                since,
                to,
                ids
            );
            errReq.errorReports.addAll(errorReports);
        }

        try
        {
            nodeRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(node -> nodes.isEmpty() ||
                    nodes.stream().anyMatch(node.getName().getDisplayName()::equalsIgnoreCase))
                .forEach(node ->
                    {
                        try
                        {
                            Peer peer = node.getPeer(apiCtx);
                            if (peer != null && peer.isConnected())
                            {
                                String nodeName = node.getName().getDisplayName();
                                errReq.requestNodes.add(nodeName);

                                byte[] msg = clientComSerializer.builder(ApiConsts.API_REQ_ERROR_REPORTS, errReq.msgReqId)
                                    .requestErrorReports(new HashSet<>(), withContent, since, to, ids).build();
                                peer.sendMessage(msg);
                            }
                        }
                        catch (AccessDeniedException ignored)
                        {
                        }
                    }
                );
        }
        catch (AccessDeniedException ignored)
        {
        }

        // no requests sent, send controller answer
        if (errReq.requestNodes.isEmpty())
        {
            client.sendMessage(clientComSerializer
                .builder(ApiConsts.API_LST_ERROR_REPORTS, errReq.msgId)
                .errorReports(errReq.errorReports)
                .build()
            );
            errorReportMap.remove(key);
        }
    }

    /**
     * Adds received error reports from satellites to the client request.
     *
     * @param nodePeer Satellite peer with error reports
     * @param msgId
     * @param rawErrorReports
     */
    void appendErrorReports(
        final Peer nodePeer,
        int msgId,
        Set<ErrorReport> rawErrorReports
    )
    {
        Pair<Peer, Integer> keyEntry = null;
        for (Map.Entry<Pair<Peer, Integer>, ErrorReportRequest> entry : errorReportMap.entrySet())
        {
            ErrorReportRequest errorReportRequest = entry.getValue();
            if (errorReportRequest.msgReqId == msgId)
            {
                errorReportRequest.errorReports.addAll(rawErrorReports);

                // remove request for node
                String nodeName = nodePeer.getNode().getName().getDisplayName();
                errorReportRequest.requestNodes.remove(nodeName);

                // if we received all error reports from requested nodes, answer our client
                if (errorReportRequest.requestNodes.isEmpty()) {
                    keyEntry = entry.getKey();
                    keyEntry.objA.sendMessage(clientComSerializer
                        .builder(ApiConsts.API_LST_ERROR_REPORTS, errorReportRequest.msgId)
                        .errorReports(errorReportRequest.errorReports)
                        .build()
                    );
                    break;
                }
            }
        }

        // remove fulfilled request
        if (keyEntry != null)
        {
            errorReportMap.remove(keyEntry);
        }
    }

    void respondNode(int msgId, UUID nodeUuid, String nodeNameStr)
    {
        try
        {
            Peer currentPeer = peer.get();
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = nodeRepository.get(apiCtx, nodeName);
            if (node != null && !node.isDeleted() && node.getFlags().isUnset(apiCtx, NodeFlag.DELETE))
            {
                if (node.getUuid().equals(nodeUuid))
                {
                    Collection<Node> otherNodes = new TreeSet<>();
                    // otherNodes can be filled with all nodes (except the current 'node')
                    // related to the satellite. The serializer only needs the other nodes for
                    // the nodeConnections.
                    for (Resource rsc : currentPeer.getNode().streamResources(apiCtx).collect(toList()))
                    {
                        Iterator<Resource> otherRscIterator = rsc.getDefinition().iterateResource(apiCtx);
                        while (otherRscIterator.hasNext())
                        {
                            Resource otherRsc = otherRscIterator.next();
                            if (otherRsc != rsc)
                            {
                                otherNodes.add(otherRsc.getAssignedNode());
                            }
                        }
                    }
                    long fullSyncTimestamp = currentPeer.getFullSyncId();
                    long serializerId = currentPeer.getNextSerializerId();
                    currentPeer.sendMessage(
                        ctrlStltSerializer
                            .builder(InternalApiConsts.API_APPLY_NODE, msgId)
                            .nodeData(node, otherNodes, fullSyncTimestamp, serializerId)
                            .build()
                    );
                }
                else
                {
                    errorReporter.reportError(
                        new ImplementationError(
                            currentPeer + " requested a node with an outdated " +
                            "UUID. Current UUID: " + node.getUuid() + ", satellites outdated UUID: " +
                            nodeUuid,
                            null
                        )
                    );
                }
            }
            else
            {
                long fullSyncTimestamp = currentPeer.getFullSyncId();
                long serializerId = currentPeer.getNextSerializerId();
                currentPeer.sendMessage(
                    ctrlStltSerializer.builder(InternalApiConsts.API_APPLY_NODE_DELETED, msgId)
                        .deletedNodeData(nodeNameStr, fullSyncTimestamp, serializerId)
                        .build()
                );
            }
        }
        catch (Exception exc)
        {
            errorReporter.reportError(
                new ImplementationError(exc)
            );
        }
    }

    private NodeType asNodeType(String nodeTypeStr)
    {
        NodeType nodeType;
        try
        {
            nodeType = NodeType.valueOfIgnoreCase(nodeTypeStr, NodeType.SATELLITE);
        }
        catch (IllegalArgumentException illegalArgExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_INVLD_NODE_TYPE,
                    "The specified node type '" + nodeTypeStr + "' is invalid."
                )
                .setCorrection("Valid node types are:\n" +
                    NodeType.CONTROLLER.name() + "\n" +
                    NodeType.SATELLITE.name() + "\n" +
                    NodeType.COMBINED.name() + "\n" +
                    NodeType.AUXILIARY.name() + "\n")
                .build(),
                illegalArgExc
            );
        }
        return nodeType;
    }

    private NodeData createNode(NodeName nodeName, NodeType type)
    {
        NodeData node;
        try
        {
            node = nodeDataFactory.create(
                peerAccCtx.get(),
                nodeName,
                type,
                new NodeFlag[0]
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // accDeniedExc during creation means that an objectProtection already exists
            // and gives no permission to the accCtx to access it.
            // This means we have an existing objProt without corresponding Node --> exception
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_ACC_DENIED_NODE,
                    "ObjProt of non-existing Node denies access of registering the Node in question."
                ),
                new LinStorException(
                    "An accessDeniedException occured during creation of a node. That means the " +
                        "ObjectProtection (of the non-existing Node) denied access to the node. " +
                        "It is possible that someone has modified the database accordingly. Please " +
                        "file a bug report otherwise.",
                    accDeniedExc
                ));
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_EXISTS_NODE,
                    "Registration of node '" + nodeName.displayValue + "' failed."
                )
                .setCause("A node with the specified name '" + nodeName.displayValue + "' already exists.")
                .setCorrection("- Specify another name for the new node\n" +
                    "or\n" +
                    "- Delete the existing node before creating a new node with the same name")
                .build(),
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return node;
    }

    private EncryptionType asEncryptionType(String encryptionTypeStr)
    {
        EncryptionType encryptionType;
        try
        {
            encryptionType = EncryptionType.valueOfIgnoreCase(encryptionTypeStr);
        }
        catch (Exception exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_INVLD_NET_TYPE,
                "The given encryption type '" + encryptionTypeStr + "' is invalid."
            ), exc);
        }
        return encryptionType;
    }

    private void reportMissingNetInterfaces(String nodeNameStr)
    {
        throw new ApiRcException(ApiCallRcImpl
            .entryBuilder(ApiConsts.FAIL_MISSING_NETCOM, "Registration of node '" + nodeNameStr + "' failed.")
            .setCause("No network interfaces were given.")
            .setCorrection("At least one network interface has to be given and be marked to be used for " +
            "controller-satellite communitaction.")
            .build()
        );
    }

    private NetInterfaceData createNetInterface(
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        TcpPortNumber port,
        EncryptionType type
    )
    {
        NetInterfaceData netIf;
        try
        {
            netIf = netInterfaceDataFactory.create(
                peerAccCtx.get(),
                node,
                netName,
                addr,
                port,
                type
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the netinterface '" + netName + "' on node '" + node.getName() + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_EXISTS_NET_IF, "Registration of node '" + node.getName() + "' failed.")
                .setCause("A duplicate network interface name was encountered during node registration.")
                .setDetails("The network interface name '" + netName +
                    "' was specified for more than one network interface.")
                .setCorrection("A name that is unique per node must be specified for each network interface.")
                .build(),
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        return netIf;
    }

    private Stream<Resource> getRscStream(NodeData nodeData)
    {
        Stream<Resource> stream;
        try
        {
            stream = nodeData.streamResources(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return stream;
    }

    private void markDeleted(Resource rsc)
    {
        try
        {
            rsc.markDeleted(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private Iterator<StorPool> getStorPoolIterator(NodeData node)
    {
        Iterator<StorPool> iterateStorPools;
        try
        {
            // Shallow-copy the storage pool map, because the Iterator is used for
            // Node.delete(), which removes objects from the original map
            Map<StorPoolName, StorPool> storPoolMap = new TreeMap<>();
            node.copyStorPoolMap(apiCtx, storPoolMap);

            iterateStorPools = storPoolMap.values().iterator();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return iterateStorPools;
    }

    private boolean hasVolumes(StorPool storPool)
    {
        boolean hasVolumes;
        try
        {
            hasVolumes = !storPool.getVolumes(apiCtx).isEmpty();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return hasVolumes;
    }

    private void delete(StorPool storPool)
    {
        try
        {
            storPool.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void delete(Resource rsc)
    {
        try
        {
            rsc.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void markDeleted(NodeData node)
    {
        try
        {
            node.markDeleted(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete the node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void delete(NodeData node)
    {
        try
        {
            node.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete the node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void setNodeType(NodeData node, String nodeTypeStr)
    {
        NodeType nodeType = asNodeType(nodeTypeStr);
        try
        {
            node.setNodeType(peerAccCtx.get(), nodeType);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "update the node type",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void requireNodesMapChangeAccess()
    {
        try
        {
            nodeRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any nodes",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
    }

    public static String getNodeDescription(String nodeNameStr)
    {
        return "Node: " + nodeNameStr;
    }

    public static String getNodeDescriptionInline(Node node)
    {
        return getNodeDescriptionInline(node.getName().displayValue);
    }

    public static String getNodeDescriptionInline(String nodeNameStr)
    {
        return "node '" + nodeNameStr + "'";
    }

    private static ResponseContext makeNodeContext(
        Peer peer,
        ApiOperation operation,
        String nodeNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);

        return new ResponseContext(
            peer,
            operation,
            getNodeDescription(nodeNameStr),
            getNodeDescriptionInline(nodeNameStr),
            ApiConsts.MASK_NODE,
            objRefs
        );
    }
}
