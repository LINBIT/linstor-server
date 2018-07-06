package com.linbit.linstor.core.apicallhandler.controller;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.nio.file.Paths;
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

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
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
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.core.apicallhandler.AbsApiCallHandler;
import com.linbit.linstor.logging.ErrorReport;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.utils.Pair;

import static java.util.stream.Collectors.toList;

public class CtrlNodeApiCallHandler extends AbsApiCallHandler
{
    private String currentNodeName;
    private final CtrlClientSerializer clientComSerializer;
    private final CoreModule.NodesMap nodesMap;
    private final ObjectProtection nodesMapProt;
    private final SatelliteConnector satelliteConnector;
    private final NodeDataControllerFactory nodeDataFactory;
    private final NetInterfaceDataFactory netInterfaceDataFactory;

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
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        CoreModule.NodesMap nodesMapRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef,
        SatelliteConnector satelliteConnectorRef,
        CtrlObjectFactories objectFactories,
        NodeDataControllerFactory nodeDataFactoryRef,
        NetInterfaceDataFactory netInterfaceDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps propsWhiteListRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            LinStorObject.NODE,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            propsWhiteListRef
        );
        clientComSerializer = clientComSerializerRef;
        nodesMap = nodesMapRef;
        nodesMapProt = nodesMapProtRef;
        satelliteConnector = satelliteConnectorRef;
        nodeDataFactory = nodeDataFactoryRef;
        netInterfaceDataFactory = netInterfaceDataFactoryRef;
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
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                nodeNameStr
            )
        )
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = asNodeName(nodeNameStr);

            NodeType type = asNodeType(nodeTypeStr);

            Node node = createNode(nodeName, type);

            fillProperties(propsMap, getProps(node), ApiConsts.FAIL_ACC_DENIED_NODE);

            if (netIfs.isEmpty())
            {
                // TODO for auxiliary nodes maybe no netif required?
                reportMissingNetInterfaces();
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
                        port = asTcpPortNumber(netIfApi.getSatelliteConnectionPort());
                        encrType = asEncryptionType(netIfApi.getSatelliteConnectionEncryptionType());
                    }

                    NetInterfaceData netIf = createNetInterface(
                        node,
                        asNetInterfaceName(netIfApi.getName()),
                        asLsIpAddress(netIfApi.getAddress()),
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
                    addAnswer(
                        "No satellite connection defined for " + getObjectDescriptionInline(),
                        ApiConsts.WARN_NO_STLT_CONN_DEFINED
                    );
                }

                commit();
                nodesMap.put(nodeName, node);

                reportSuccess(node.getUuid());

                if (type.equals(NodeType.SATELLITE) || type.equals(NodeType.COMBINED))
                {
                    satelliteConnector.startConnecting(node, peerAccCtx);
                }
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.CREATE,
                getObjectDescriptionInline(nodeNameStr),
                getObjRefs(nodeNameStr),
                apiCallRc
            );
        }
        return apiCallRc;
    }

    private void setCurrentStltConn(Node node, NetInterfaceData netIf)
    {
        try
        {
            node.setSatelliteConnection(peerAccCtx, netIf);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "set the current satellite connection of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException exc)
        {
            throw asSqlExc(
                exc,
                "setting the current satellite connection of " + getObjectDescriptionInline()
            );
        }
    }

    private NetInterface getCurrentStltConn(Node node)
    {
        NetInterface netIf = null;
        try
        {
            netIf = node.getSatelliteConnection(peerAccCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw asAccDeniedExc(
                exc,
                "access the current satellite connection of " + getObjectDescriptionInline(),
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
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.MODIFY,
                apiCallRc,
                nodeNameStr
            )
        )
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = asNodeName(nodeNameStr);
            NodeData node = loadNode(nodeName, true);
            if (nodeUuid != null && !nodeUuid.equals(node.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_NODE
                );
                throw new ApiCallHandlerFailedException();
            }
            if (nodeTypeStr != null)
            {
                setNodeType(node, nodeTypeStr);
            }

            Props props = getProps(node);
            fillProperties(overrideProps, props, ApiConsts.FAIL_ACC_DENIED_NODE);

            Map<String, String> nodeProps = props.map();
            for (String key : deletePropKeys)
            {
                nodeProps.remove(key);
            }

            commit();

            updateSatellites(node);
            reportSuccess(node.getUuid());
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr),
                getObjRefs(nodeNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    ApiCallRc deleteNode(String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallythis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeNameStr
            )
        )
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = asNodeName(nodeNameStr);
            NodeData nodeData = loadNode(nodeName, false);
            if (nodeData == null)
            {
                addAnswer(
                    "Deletion of node '" + nodeName + "' had no effect.",
                    "Node '" + nodeName + "' does not exist.",
                    null,
                    null,
                    ApiConsts.WARN_NOT_FOUND
                );
                throw new ApiCallHandlerFailedException();
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
                            addAnswer(
                                String.format(
                                    "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                                    "on this node, although the node does not reference any resources",
                                    nodeNameStr,
                                    storPool.getName().displayValue
                                ),
                                ApiConsts.FAIL_EXISTS_VLM
                            );
                        }
                    }
                }

                if (success)
                {
                    String successMessage = getObjectDescriptionInlineFirstLetterCaps();
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

                    commit();

                    if (!hasRsc)
                    {
                        nodesMap.remove(nodeName);

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
                            updateSatellites(rscDfn);

                            // when they finished undeploying the resource(s), the corresponding
                            // "resourceDeleted" method in CtrlRscApiCallHandler will check if the
                            // node also needs to be deleted, and does so if needed.
                        }
                        updateSatellites(nodeData);
                    }

                    reportSuccess(
                        successMessage,
                        getObjectDescriptionInlineFirstLetterCaps() + " UUID is: " + nodeUuid.toString()
                    );

                    // TODO: tell satellites to remove all the corresponding resources and storPools
                    // TODO: if satellites finished, cleanup the storPools and then remove the node from DB
                }
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(nodeNameStr),
                getObjRefs(nodeNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    ApiCallRc lostNode(String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallythis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                nodeNameStr
            )
        )
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = asNodeName(nodeNameStr);
            NodeData nodeData = loadNode(nodeName, false);
            if (nodeData == null)
            {
                addAnswer(
                    "Deletion of node '" + nodeName + "' had no effect.",
                    "Node '" + nodeName + "' does not exist.",
                    null,
                    null,
                    ApiConsts.WARN_NOT_FOUND
                );
                throw new ApiCallHandlerFailedException();
            }
            else
            {
                Peer nodePeer = nodeData.getPeer(apiCtx);
                if (nodePeer != null && nodePeer.isConnected())
                {
                    addAnswer(
                        String.format(
                            "Node '%s' still connected, please use the '%s' api.",
                            nodeNameStr,
                            ApiConsts.API_DEL_NODE
                        ),
                        ApiConsts.FAIL_EXISTS_NODE_CONN
                    );
                    throw new ApiCallHandlerFailedException();
                }

                // set node mark deleted for updates to other satellites
                nodeData.markDeleted(apiCtx);
                // inform other satellites that the node is gone
                updateSatellites(nodeData);

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
                        addAnswer(
                            String.format(
                                "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                                    "on this node, although the node does not reference any resources",
                                nodeNameStr,
                                storPool.getName().displayValue
                            ),
                            ApiConsts.FAIL_EXISTS_VLM
                        );
                        throw new ApiCallHandlerFailedException();
                    }
                }


                String successMessage = getObjectDescriptionInlineFirstLetterCaps() + " deleted.";
                UUID nodeUuid = nodeData.getUuid(); // store node uuid to avoid deleted node access

                delete(nodeData);

                commit();

                nodesMap.remove(nodeName);

                reportSuccess(
                    successMessage,
                    getObjectDescriptionInlineFirstLetterCaps() + " UUID is: " + nodeUuid.toString()
                );
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception | ImplementationError exc)
        {
            reportStatic(
                exc,
                ApiCallType.DELETE,
                getObjectDescriptionInline(nodeNameStr),
                getObjRefs(nodeNameStr),
                apiCallRc
            );
        }

        return apiCallRc;
    }

    byte[] listNodes(int msgId)
    {
        ArrayList<Node.NodeApi> nodes = new ArrayList<>();
        try
        {
            nodesMapProt.requireAccess(peerAccCtx, AccessType.VIEW); // accDeniedExc1
            for (Node node : nodesMap.values())
            {
                try
                {
                    nodes.add(node.getApiData(peerAccCtx, null, null));
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

        nodesMap.values().stream()
            .filter(node -> nodes.isEmpty() ||
                nodes.stream().anyMatch(node.getName().getDisplayName()::equalsIgnoreCase))
            .forEach( node ->
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

            Node node = nodesMap.get(nodeName);
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
                        internalComSerializer
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
                    internalComSerializer.builder(InternalApiConsts.API_APPLY_NODE_DELETED, msgId)
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

    private NodeType asNodeType(String nodeTypeStr) throws ApiCallHandlerFailedException
    {
        NodeType nodeType;
        try
        {
            nodeType = NodeType.valueOfIgnoreCase(nodeTypeStr, NodeType.SATELLITE);
        }
        catch (IllegalArgumentException illegalArgExc)
        {
            throw asExc(
                illegalArgExc,
                "The specified node type '" + nodeTypeStr + "' is invalid.",
                null, // cause
                null, // details
                "Valid node types are:\n" +
                NodeType.CONTROLLER.name() + "\n" +
                NodeType.SATELLITE.name() + "\n" +
                NodeType.COMBINED.name() + "\n" +
                NodeType.AUXILIARY.name() + "\n", // correction
                ApiConsts.FAIL_INVLD_NODE_TYPE
            );
        }
        return nodeType;
    }

    private NodeData createNode(NodeName nodeName, NodeType type)
        throws ApiCallHandlerFailedException
    {
        NodeData node;
        try
        {
            node = nodeDataFactory.getInstance(
                peerAccCtx,
                nodeName,
                type,
                new NodeFlag[0],
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // accDeniedExc during creation means that an objectProtection already exists
            // and gives no permission to the accCtx to access it.
            // This means we have an existing objProt without corresponding Node --> exception
            throw asExc(
                new LinStorException(
                    "An accessDeniedException occured during creation of a node. That means the " +
                        "ObjectProtection (of the non-existing Node) denied access to the node. " +
                        "It is possible that someone has modified the database accordingly. Please " +
                        "file a bug report otherwise.",
                    accDeniedExc
                ),
                "ObjProt of non-existing Node denies access of creating the Node in question.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                "Creation of node '" + nodeName.displayValue + "' failed.",
                "A node with the specified name '" + nodeName.displayValue + "' already exists.",
                null,
                "- Specify another name for the new node\n" +
                "or\n" +
                "- Delete the existing node before creating a new node with the same name",
                ApiConsts.FAIL_EXISTS_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
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
            throw asExc(
                exc,
                "The given encryption type '" + encryptionTypeStr + "' is invalid.",
                ApiConsts.FAIL_INVLD_NET_TYPE
            );
        }
        return encryptionType;
    }

    private void reportMissingNetInterfaces()
    {
        throw asExc(
            null,
            "Creation of node '" + currentNodeName + "' failed.",
            "No network interfaces were given.",
            null,
            "At least one network interface has to be given and be marked to be used for " +
            "controller-satellite communitaction.",
            ApiConsts.FAIL_MISSING_NETCOM
        );
    }

    private void reportMissingSatelliteConnection()
    {
        throw asExc(
            null,
            "Creation of node '" + currentNodeName + "' failed.",
            "No network interfaces was specified as satellite connection.",
            null,
            "At least one network interface has to be given and be marked to be used for " +
            "controller-satellite communitaction.",
            ApiConsts.FAIL_MISSING_STLT_CONN
        );
    }

    private NetInterfaceData createNetInterface(
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        TcpPortNumber port,
        EncryptionType type
    )
        throws ApiCallHandlerFailedException
    {
        NetInterfaceData netIf;
        try
        {
            netIf = netInterfaceDataFactory.getInstance(
                peerAccCtx,
                node,
                netName,
                addr,
                port,
                type,
                true,   // persist node
                true    // throw LinStorDataAlreadyExistsException if needed
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create the netinterface '" + netName + "' on node '" + node.getName() + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                "Creation of node '" + node.getName() + "' failed.",
                "A duplicate network interface name was encountered during node creation.",
                "The network interface name '" + netName + "' was specified for more than one network interface.",
                "A name that is unique per node must be specified for each network interface.",
                ApiConsts.FAIL_EXISTS_NET_IF
            );
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
        }
        return netIf;
    }

    private NetInterface getNetInterface(Node node, NetInterfaceName niName)
    {
        NetInterface netInterface;
        try
        {
            netInterface = node.getNetInterface(peerAccCtx, niName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "creating a satellite connection",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return netInterface;
    }

    private Stream<Resource> getRscStream(NodeData nodeData) throws ApiCallHandlerFailedException
    {
        Stream<Resource> stream;
        try
        {
            stream = nodeData.streamResources(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return stream;
    }

    private void markDeleted(Resource rsc) throws ApiCallHandlerFailedException
    {
        try
        {
            rsc.markDeleted(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "An SQLException occured while marking resource '" + rsc.getDefinition().getName().displayValue +
                "' on node '" + currentNodeName + "' as deleted "
            );
        }
    }

    private Iterator<StorPool> getStorPoolIterator(NodeData node) throws ApiCallHandlerFailedException
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
            throw asImplError(accDeniedExc);
        }
        return iterateStorPools;
    }

    private boolean hasVolumes(StorPool storPool) throws ApiCallHandlerFailedException
    {
        boolean hasVolumes;
        try
        {
            hasVolumes = !storPool.getVolumes(apiCtx).isEmpty();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return hasVolumes;
    }

    private void delete(StorPool storPool) throws ApiCallHandlerFailedException
    {
        try
        {
            storPool.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
        }
    }

    private void delete(Resource rsc) throws ApiCallHandlerFailedException
    {
        try
        {
            rsc.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
        }
    }

    private void markDeleted(NodeData node) throws ApiCallHandlerFailedException
    {
        try
        {
            node.markDeleted(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete the node '" + currentNodeName + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
        }
    }

    private void delete(NodeData node) throws ApiCallHandlerFailedException
    {
        try
        {
            node.delete(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete the node '" + currentNodeName + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
        }
    }

    private void setNodeType(NodeData node, String nodeTypeStr)
    {
        NodeType nodeType = asNodeType(nodeTypeStr);
        try
        {
            node.setNodeType(peerAccCtx, nodeType);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "update the node type",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        catch (SQLException sqlExc)
        {
            throw handleSqlExc(sqlExc);
        }
    }

    private ApiCallHandlerFailedException handleSqlExc(SQLException sqlExc)
    {
        throw asSqlExc(
            sqlExc,
            getAction(
                "creating node '" + currentNodeName + "'",
                "deleting node '" + currentNodeName + "'",
                "modifying node '" + currentNodeName + "'"
            )
        );
    }

    private AbsApiCallHandler setContext(
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        String nodeNameStr
    )
    {
        super.setContext(
            apiCallType,
            apiCallRc,
            true, // autoClose
            getObjRefs(nodeNameStr)
        );
        currentNodeName = nodeNameStr;
        return this;
    }

    private Map<String, String> getObjRefs(String nodeNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE, nodeNameStr);
        return map;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(currentNodeName);
    }

    protected void requireNodesMapChangeAccess() throws ApiCallHandlerFailedException
    {
        try
        {
            nodesMapProt.requireAccess(
                peerAccCtx,
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                getAction("create", "modify", "delete") + " node entries",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
    }

    static String getObjectDescriptionInline(String nodeNameStr)
    {
        return "node '" + nodeNameStr + "'";
    }
}
