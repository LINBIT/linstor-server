package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
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
import com.linbit.linstor.SatelliteConnection.EncryptionType;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.SatelliteConnectionDataFactory;
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
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Stream;

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
    private final SatelliteConnectionDataFactory satelliteConnectionDataFactory;

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
        SatelliteConnectionDataFactory satelliteConnectionDataFactoryRef,
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
        satelliteConnectionDataFactory = satelliteConnectionDataFactoryRef;
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
     * @param accCtx
     * @param client
     * @param nodeNameStr
     * @param nodeTypeStr
     * @param netIfs
     * @param satelliteConnectionApis
     * @param propsMap
     * @return
     */
    public ApiCallRc createNode(
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        List<SatelliteConnectionApi> satelliteConnectionApis,
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
            if (satelliteConnectionApis.isEmpty())
            {
                // TODO for auxiliary nodes maybe no stltConn required?
                reportMissingSatelliteConnection();
            }
            else
            {
                Map<String, NetInterface> netIfMap = new TreeMap<>();

                for (NetInterfaceApi netIfApi : netIfs)
                {
                    NetInterfaceData netIf = createNetInterface(
                        node,
                        asNetInterfaceName(netIfApi.getName()),
                        asLsIpAddress(netIfApi.getAddress())
                    );
                    netIfMap.put(netIfApi.getName(), netIf);
                }

                SatelliteConnectionApi stltConnApi = satelliteConnectionApis.iterator().next();

                createSatelliteConnection(
                    node,
                    stltConnApi
                );

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
                getVariables(nodeNameStr),
                apiCallRc
            );
        }
        return apiCallRc;
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
                getVariables(nodeNameStr),
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

                for (Resource rsc : getRscStream(nodeData).collect(toList()))
                {
                    hasRsc = true;
                    markDeleted(rsc);
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
                        nodeData.getPeer(apiCtx).closeConnection();
                    }
                    else
                    {
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
                getVariables(nodeNameStr),
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

    void respondNode(int msgId, UUID nodeUuid, String nodeNameStr)
    {
        try
        {
            Peer currentPeer = peer.get();
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = nodesMap.get(nodeName);
            if (node != null)
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
                            "Satellite '" + currentPeer.getId() + "' requested a node with an outdated " +
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
        LsIpAddress addr
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

    private void createSatelliteConnection(
        Node node,
        SatelliteConnectionApi stltConnApi
    )
    {
        NetInterfaceName stltNetIfName = asNetInterfaceName(stltConnApi.getNetInterfaceName());
        TcpPortNumber stltNetIfPort = asTcpPortNumber(stltConnApi.getPort());
        EncryptionType stltNetIfEncryptionType = asEncryptionType(stltConnApi.getEncryptionType());

        NetInterface netIf = getNetInterface(node, stltNetIfName);

        try
        {

            satelliteConnectionDataFactory.getInstance(
                peerAccCtx,
                node,
                netIf,
                stltNetIfPort,
                stltNetIfEncryptionType,
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "creating a satellite connection",
                ApiConsts.FAIL_ACC_DENIED_STLT_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException alreadyExistsExc)
        {
            throw new ImplementationError(
                "New node already had an satellite connection",
                alreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(sqlExc, "creating a satellite connection");
        }
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
            getObjRefs(nodeNameStr),
            getVariables(nodeNameStr)
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

    private Map<String, String> getVariables(String nodeNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
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
