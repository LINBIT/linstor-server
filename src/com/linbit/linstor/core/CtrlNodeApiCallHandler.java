package com.linbit.linstor.core;

import java.io.IOException;
import java.net.InetSocketAddress;
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

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorRuntimeException;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.SatelliteConnection;
import com.linbit.linstor.SatelliteConnection.EncryptionType;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.SatelliteConnectionData;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlNodeSerializer;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

class CtrlNodeApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentNodeType = new ThreadLocal<>();
    private final CtrlNodeSerializer nodeSerializer;
    private final CtrlListSerializer<Node.NodeApi> nodeListSerializer;

    CtrlNodeApiCallHandler(
        Controller controllerRef,
        AccessContext apiCtxRef,
        CtrlNodeSerializer nodeSerializer,
        CtrlListSerializer<Node.NodeApi> nodeListSerializer
    )
    {
        super(controllerRef, apiCtxRef, ApiConsts.MASK_NODE);
        this.nodeSerializer = nodeSerializer;
        this.nodeListSerializer = nodeListSerializer;
    }

    @Override
    protected CtrlNodeSerializer getNodeSerializer()
    {
        return nodeSerializer;
    }

    ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String nodeTypeStr,
        List<NetInterfaceApi> netIfs,
        List<SatelliteConnectionApi> satelliteConnectionApis,
        Map<String, String> propsMap
    )
    {
        /*
         * Usually its better to handle exceptions "close" to their appearance.
         * However, as in this method almost every other line throws an exception,
         * the code would get completely unreadable; thus, unmaintainable.
         *
         * For that reason there is (almost) only one try block with many catches, and
         * those catch blocks handle the different cases (commented as <some>Exc<count> in
         * the try block and a matching "handle <some>Exc<count>" in the catch block)
         */

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null, // create new TransactionMgr
                nodeNameStr,
                nodeTypeStr
            )
        )
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = asNodeName(nodeNameStr);

            NodeType type = asNodeType(nodeTypeStr);

            Node node = createNode(nodeName, type);

            Props nodeProps = getProps(node);
            nodeProps.map().putAll(propsMap);

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
                controller.nodesMap.put(nodeName, node);

                reportSuccess(
                    "New node '" + nodeNameStr + "' created.",
                    "Node '" + nodeNameStr + "' UUID is " + node.getUuid()
                );

                if (type.equals(NodeType.SATELLITE) || type.equals(NodeType.COMBINED))
                {
                    startConnecting(node, accCtx, client, controller);
                }
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception exc)
        {
            asExc(
                exc,
                "Creation of a node object failed due to an unhandled exception.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
        }
        catch (ImplementationError implErr)
        {
            asImplError(implErr);
        }

        return apiCallRc;
    }

    ApiCallRc modifyNode(
        AccessContext accCtx,
        Peer client,
        UUID nodeUuid,
        String nodeNameStr,
        String nodeTypeStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try (
            AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.MODIFY,
                apiCallRc,
                null, // create transMgr
                nodeNameStr,
                nodeTypeStr
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
            Map<String, String> nodeProps = getProps(node).map();
            nodeProps.putAll(overrideProps);
            for (String key : deletePropKeys)
            {
                nodeProps.remove(key);
            }

            commit();

            updateSatellites(node);
            reportSuccess("Node '" + nodeNameStr + "' updated.");
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // failure was reported and added to returning apiCallRc
            // this is only for flow-control.
        }
        catch (Exception exc)
        {
            asExc(
                exc,
                "Modifying node '" + nodeNameStr + "' failed due to an unknown exception.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
        }
        catch (ImplementationError implErr)
        {
            asImplError(implErr);
        }

        return apiCallRc;
    }

    ApiCallRc deleteNode(AccessContext accCtx, Peer client, String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallythis = setCurrent(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null,
                nodeNameStr,
                null
            )
        )
        {
            requireNodesMapChangeAccess();
            NodeName nodeName = asNodeName(nodeNameStr);
            NodeData nodeData = loadNode(nodeName, true);
            if (nodeData == null)
            {
                addAnswer(
                    "Deletion of node '" + currentNodeName.get() + "' had no effect.",
                    "Node '" + currentNodeName.get() + "' does not exist.",
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

                Iterator<Resource> rscIterator = getRscIterator(nodeData);
                while (rscIterator.hasNext())
                {
                    hasRsc = true;
                    Resource rsc = rscIterator.next();
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
                    String successMessage;
                    if (hasRsc)
                    {
                        markDeleted(nodeData);
                        successMessage = String.format(
                            "Node '%s' marked for deletion.",
                            nodeNameStr
                        );
                    }
                    else
                    {
                        delete(nodeData);
                        successMessage = String.format(
                            "Node '%s' deleted.",
                            nodeNameStr
                        );
                    }

                    commit();

                    if (!hasRsc)
                    {
                        controller.nodesMap.remove(nodeName);
                    }
                    else
                    {
                        updateSatellites(nodeData);
                    }

                    reportSuccess(successMessage);

                    // TODO: tell satellites to remove all the corresponding resources and storPools
                    // TODO: if satellites finished, cleanup the storPools and then remove the node from DB
                }
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // failure was reported and added to returning apiCallRc
            // this is only for flow-control.
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            asExc(
                exc,
                String.format(
                    "An unknown exception occured while deleting node '%s'.",
                    nodeNameStr
                ),
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
        }

        return apiCallRc;
    }

    byte[] listNodes(int msgId, AccessContext accCtx, Peer client)
    {
        ArrayList<Node.NodeApi> nodes = new ArrayList<Node.NodeApi>();
        try
        {
            controller.nodesMapProt.requireAccess(accCtx, AccessType.VIEW);// accDeniedExc1
            for (Node n : controller.nodesMap.values())
            {
                try {
                    nodes.add(n.getApiData(accCtx));
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

        try
        {
            return nodeListSerializer.getListMessage(msgId, nodes);
        }
        catch (IOException e)
        {
            controller.getErrorReporter().reportError(
                e,
                null,
                client,
                "Could not complete list message due to an IOException"
            );
        }

        return null;
    }

    void respondNode(int msgId, Peer satellite, UUID nodeUuid, String nodeNameStr)
    {
        try
        {
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = controller.nodesMap.get(nodeName);
            if (node != null)
            {
                if (node.getUuid().equals(nodeUuid))
                {
                    Collection<Node> otherNodes = new TreeSet<>();
                    // otherNodes can be filled with all nodes (except the current 'node')
                    // related to the satellite. The serializer only needs the other nodes for
                    // the nodeConnections.
                    Iterator<Resource> rscIterator = satellite.getNode().iterateResources(apiCtx);
                    while (rscIterator.hasNext())
                    {
                        Resource rsc = rscIterator.next();
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
                    byte[] data = nodeSerializer.getDataMessage(msgId, node, otherNodes);
                    Message message = satellite.createMessage();
                    message.setData(data);
                    satellite.sendMessage(message);
                }
                else
                {
                    controller.getErrorReporter().reportError(
                        new ImplementationError(
                            "Satellite '" + satellite.getId() + "' requested a node with an outdated " +
                            "UUID. Current UUID: " + node.getUuid() + ", satellites outdated UUID: " +
                            nodeUuid,
                            null
                        )
                    );
                }
            }
            else
            {
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        "A requested node '" + nodeNameStr + "' was not found in controllers nodesMap",
                        null
                    )
                );
            }
        }
        catch (Exception exc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(exc)
            );
        }
    }

    public static void startConnecting(
        Node node,
        AccessContext accCtx,
        Peer client,
        Controller controller
    )
    {
        try
        {
            SatelliteConnection satelliteConnection = node.getSatelliteConnection(accCtx);
            if (satelliteConnection != null ) {
                EncryptionType type = satelliteConnection.getEncryptionType();
                String serviceType;
                switch (type)
                {
                    case PLAIN:
                        serviceType = Controller.PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC;
                        break;
                    case SSL:
                        serviceType = Controller.PROPSCON_KEY_DEFAULT_SSL_CON_SVC;
                        break;
                    default:
                        throw new ImplementationError(
                            "Unhandeld default case for EncryptionType",
                            null
                        );
                }
                ServiceName dfltConSvcName;
                try
                {
                    dfltConSvcName = new ServiceName(
                        controller.ctrlConf.getProp(serviceType)
                    );
                }
                catch (InvalidNameException invalidNameExc)
                {
                    throw new LinStorRuntimeException(
                        "The ServiceName of the default TCP connector is not valid",
                        invalidNameExc
                    );
                }
                TcpConnector tcpConnector = controller.netComConnectors.get(dfltConSvcName);

                if (tcpConnector != null)
                {
                    controller.connectSatellite(
                        new InetSocketAddress(
                            satelliteConnection.getNetInterface().getAddress(accCtx).getAddress(),
                            satelliteConnection.getPort().value
                        ),
                        tcpConnector,
                        node
                    );
                }
                else
                {
                    throw new LinStorRuntimeException(
                        "Attempt to establish a " + type + " connection without a proper connector defined"
                    );
                }
            }
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new LinStorRuntimeException(
                "Access to an object protected by access controls was revoked while a " +
                "controller<->satellite connect operation was in progress.",
                exc
            );
        }
    }

    private void requireNodesMapChangeAccess() throws ApiCallHandlerFailedException
    {
        try
        {
            controller.nodesMapProt.requireAccess(
                currentAccCtx.get(),
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

    private NodeType asNodeType(String nodeTypeStr) throws ApiCallHandlerFailedException
    {
        try
        {
            return NodeType.valueOfIgnoreCase(nodeTypeStr, NodeType.SATELLITE);
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
    }

    private NodeData createNode(NodeName nodeName, NodeType type)
        throws ApiCallHandlerFailedException
    {
        try
        {
            return NodeData.getInstance(
                currentAccCtx.get(),
                nodeName,
                type,
                new NodeFlag[0],
                currentTransMgr.get(),
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create the node '" + nodeName + "'.",
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
    }

    private NetInterfaceName asNetInterfaceName(String netIfNameStr)
        throws ApiCallHandlerFailedException
    {
        try
        {
            return new NetInterfaceName(netIfNameStr);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw asExc(
                invalidNameExc,
                "The specified net interface name '" + netIfNameStr + "' is invalid.",
                ApiConsts.FAIL_INVLD_NET_NAME
            );
        }
    }

    private LsIpAddress asLsIpAddress(String ipAddrStr)
        throws ApiCallHandlerFailedException
    {
        if (ipAddrStr == null)
        {
            throw asExc(
                null,
                "Node creation failed.",
                "No IP address for the new node was specified",
                null,
                "At least one network interface with a valid IP address must be defined for the new node.",
                ApiConsts.FAIL_INVLD_NET_ADDR
            );
        }
        try
        {
            return new LsIpAddress(ipAddrStr);
        }
        catch (InvalidIpAddressException invalidIpExc)
        {
            throw asExc(
                invalidIpExc,
                "Node creation failed.",
                "The specified IP address is not valid",
                "The specified input '" + ipAddrStr + "' is not a valid IP address.",
                "Specify a valid IPv4 or IPv6 address.",
                ApiConsts.FAIL_INVLD_NET_ADDR
            );
        }
    }

    private TcpPortNumber asTcpPortNumber(int port)
    {
        try
        {
            return new TcpPortNumber(port);
        }
        catch (Exception exc)
        {
            throw asExc(
                exc,
                "The given portNumber '" + port + "' is invalid.",
                ApiConsts.FAIL_INVLD_NET_PORT
            );
        }
    }

    private EncryptionType asEncryptionType(String encryptionTypeStr)
    {
        try
        {
            return EncryptionType.valueOfIgnoreCase(encryptionTypeStr);
        }
        catch (Exception exc)
        {
            throw asExc(
                exc,
                "The given encryption type '" + encryptionTypeStr + "' is invalid.",
                ApiConsts.FAIL_INVLD_NET_TYPE
            );
        }
    }

    private void reportMissingNetInterfaces()
    {
        throw asExc(
            null,
            "Creation of node '" + currentNodeName.get() + "' failed.",
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
            "Creation of node '" + currentNodeName.get() + "' failed.",
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
        try
        {
            return NetInterfaceData.getInstance(
                currentAccCtx.get(),
                node,
                netName,
                addr,
                currentTransMgr.get(),
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

            SatelliteConnectionData.getInstance(
                currentAccCtx.get(),
                node,
                netIf,
                stltNetIfPort,
                stltNetIfEncryptionType,
                currentTransMgr.get(),
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
        try
        {
            return node.getNetInterface(currentAccCtx.get(), niName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "creating a satellite connection",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
    }

    private Iterator<Resource> getRscIterator(NodeData nodeData) throws ApiCallHandlerFailedException
    {
        try
        {
            return nodeData.iterateResources(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private void markDeleted(Resource rsc) throws ApiCallHandlerFailedException
    {
        try
        {
            rsc.setConnection(currentTransMgr.get());
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
                "' on node '" + currentNodeName.get() + "' as deleted "
            );
        }
    }

    private Iterator<StorPool> getStorPoolIterator(NodeData node) throws ApiCallHandlerFailedException
    {
        try
        {
            return node.iterateStorPools(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private boolean hasVolumes(StorPool storPool) throws ApiCallHandlerFailedException
    {
        try
        {
            return !storPool.getVolumes(apiCtx).isEmpty();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private void delete(StorPool storPool) throws ApiCallHandlerFailedException
    {
        try
        {
            storPool.setConnection(currentTransMgr.get());
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
            node.markDeleted(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete the node '" + currentNodeName.get() + "'.",
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
            node.delete(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete the node '" + currentNodeName.get() + "'.",
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
            node.setNodeType(currentAccCtx.get(), nodeType);
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
                "creating node '" + currentNodeName.get() + "'",
                "deleting node '" + currentNodeName.get() + "'",
                "modifying node '" + currentNodeName.get() + "'"
            )
        );
    }

    private AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer client,
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeNameStr,
        String nodeTypeStr
    )
    {
        super.setCurrent(accCtx, client, apiCallType, apiCallRc, transMgr);
        currentNodeName.set(nodeNameStr);
        currentNodeType.set(nodeTypeStr);
        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeName.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return "node '" + currentNodeName.get() +"'";
    }
}
