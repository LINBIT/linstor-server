package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.*;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.NetInterface.NetInterfaceType;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
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

    CtrlNodeApiCallHandler(Controller controllerRef, AccessContext apiCtxRef)
    {
        super(controllerRef, apiCtxRef);
    }

    ApiCallRc createNode(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String nodeTypeStr,
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


        TransactionMgr transMgr = null;
        Node node = null;
        NodeType type = null;
        NodeName nodeName = null;
        Props nodeProps = null;
        String currentNetIfNameStr = null;
        String netComTypeStr = null;
        String portStr = null;
        Integer port = null;
        boolean enabled = true;
        NetInterfaceType netType = null;
        Props netIfProps = null;
        try (
            AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                nodeNameStr,
                nodeTypeStr
            )
        )
        {
            requireNodesMapChangeAccess();
            transMgr = createNewTransMgr();
            nodeName = asNodeName(nodeNameStr);

            type = asNodeType(nodeTypeStr);

            node = createNode(nodeName, type, transMgr);
            node.setConnection(transMgr);

            nodeProps = node.getProps(accCtx); // accDeniedExc (implError)
            nodeProps.map().putAll(propsMap);

            netIfProps = nodeProps.getNamespace(NAMESPC_NETIF);
            if (netIfProps == null)
            {
                // TODO for auxiliary nodes maybe the netIf-namespace is not required?
                reportMissingNetIfNamespace();
            }
            else
            {
                Iterator<String> netIfNamesIterator = netIfProps.iterateNamespaces();
                boolean atLeastOneEnabled = false;
                while (netIfNamesIterator.hasNext())
                {
                    currentNetIfNameStr = netIfNamesIterator.next();
                    portStr = null;
                    port = null;
                    netComTypeStr = null;
                    netType = null;
                    enabled = true;

                    NetInterfaceName netName = asNetInterfaceName(currentNetIfNameStr);
                    LsIpAddress addr = asDmIpAddress(netIfProps.getProp(KEY_IP_ADDR, currentNetIfNameStr));

                    portStr = netIfProps.getProp(KEY_PORT_NR, currentNetIfNameStr);
                    netComTypeStr = netIfProps.getProp(KEY_NETCOM_TYPE, currentNetIfNameStr);
                    if (portStr == null || netComTypeStr == null)
                    {
                        if (portStr == null && netComTypeStr != null)
                        {
                            reportMissingPort(currentNetIfNameStr);
                        }
                        else
                        if (portStr != null && netComTypeStr == null)
                        {
                            reportMissingNetComType(currentNetIfNameStr);
                        }
                        // TODO if both are null, it is still valid (not as a LinStor connection, but as a drbd connection)
                    }
                    else
                    {
                        port = asPort(portStr, currentNetIfNameStr);
                        netType = getNetIfType(netIfProps, currentNetIfNameStr, NetInterfaceType.IP);
                        enabled = isEnabled(netIfProps, currentNetIfNameStr, true);

                        if (enabled)
                        {
                            atLeastOneEnabled = true;
                            createNetInterface(
                                node,
                                netName,
                                addr,
                                port,
                                netType,
                                transMgr
                            );
                        }
                    }
                }

                if (atLeastOneEnabled == false)
                {
                    reportMissingEnabledNetCom();
                }
                else
                {
                    commit(transMgr);
                    controller.nodesMap.put(nodeName, node);

                    String successMessage = String.format(
                        "New node '%s' created.",
                        nodeNameStr
                    );
                    String detailsMessage = "Node '" + nodeNameStr + "' UUID is " + node.getUuid();
                    addAnswer(successMessage, null, detailsMessage, null, RC_NODE_CREATED);
                    controller.getErrorReporter().logInfo(successMessage);

                    if (type.equals(NodeType.SATELLITE) || type.equals(NodeType.COMBINED))
                    {
                        startConnecting(node, accCtx, client);
                    }
                }
            }
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // a report and a corresponding api-response already created. nothing to do here
        }
        catch (Exception exc)
        {
            report(
                exc,
                "Creation of a node object failed due to an unhandled exception.",
                RC_NODE_CRT_FAIL_UNKNOWN_ERROR
            );
        }
        catch (ImplementationError implErr)
        {
            reportImplError(implErr);
        }

        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    String errorMessage = String.format(
                        "A database error occured while trying to rollback the creation of " +
                        "node '%s'.",
                        nodeNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_NODE_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_NODE, nodeNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    private void startConnecting(Node node, AccessContext accCtx, Peer client)
    {
        Props netIfProps;
        try
        {
            netIfProps = node.getProps(accCtx).getNamespace(NAMESPC_NETIF);
            Iterator<String> iterator = netIfProps.iterateNamespaces();
            TcpConnector tcpConnector = null;
            InetSocketAddress satelliteAddress = null;
            while (iterator.hasNext() && tcpConnector == null)
            {
                String netIf = iterator.next();
                try
                {
                    if (!VAL_FALSE.equalsIgnoreCase(netIfProps.getProp(KEY_NETCOM_ENABLED, netIf)))
                    {
                        String addr = netIfProps.getProp(KEY_IP_ADDR, netIf);
                        String port = netIfProps.getProp(KEY_PORT_NR, netIf);
                        String type = netIfProps.getProp(KEY_NETCOM_TYPE, netIf);

                        satelliteAddress = new InetSocketAddress(addr, Integer.parseInt(port));
                        String serviceType;
                        if (type.equals(VAL_NETCOM_TYPE_PLAIN))
                        {
                            serviceType = Controller.PROPSCON_KEY_DEFAULT_PLAIN_CON_SVC;
                        }
                        else
                        {
                            serviceType = Controller.PROPSCON_KEY_DEFAULT_SSL_CON_SVC;
                        }
                        String dfltConSvc = controller.ctrlConf.getProp(serviceType);
                        if (dfltConSvc == null)
                        {
                            // TODO: Add correction instructions for adding a default TCP connector
                            controller.getErrorReporter().reportError(
                                new LinStorException(
                                    "The controller has no default " + type.toLowerCase() + " tcp connector "
                                ),
                                accCtx,
                                client,
                                "The controller cannot connect to satellite nodes because no " +
                                "default TCP connector is defined"
                            );
                        }
                        ServiceName dfltConSvcName = new ServiceName(dfltConSvc);
                        tcpConnector = controller.netComConnectors.get(dfltConSvcName);
                    }
                }
                catch (NumberFormatException numberFormatExc)
                {
                    // ignore and try the next one
                }
                catch (InvalidNameException invalidNameExc)
                {
                    controller.getErrorReporter().reportError(
                        new LinStorException(
                            "The ServiceName of the default TCP connector is not valid",
                            invalidNameExc
                        )
                    );
                }
                catch (InvalidKeyException invalidKeyExc)
                {
                    controller.getErrorReporter().reportError(
                        new ImplementationError(
                            "A constant for a PropsContainer key generated an InvalidKeyException",
                            invalidKeyExc
                        )
                    );
                }
            }

            if (satelliteAddress != null && tcpConnector != null)
            {
                controller.connectSatellite(satelliteAddress, tcpConnector, node);
            }
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            controller.getErrorReporter().reportError(
                new LinStorException(
                    "Access to an object protected by access controls was revoked while a " +
                    "controller<->satellite connect operation was in progress.", exc
                )
            );
        }
    }

    ApiCallRc deleteNode(AccessContext accCtx, Peer client, String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;
        NodeData nodeData = null;
        Iterator<Resource> rscIterator = null;
        Iterator<StorPool> storPoolIterator = null;

        try
        {
            controller.nodesMapProt.requireAccess(accCtx, AccessType.CHANGE);// accDeniedExc1
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection());// sqlExc1
            NodeName nodeName = new NodeName(nodeNameStr); // invalidNameExc1
            nodeData = NodeData.getInstance( // sqlExc2, accDeniedExc2, dataAlreadyExistsExc1
                accCtx,
                nodeName,
                null, null,
                transMgr,
                false,
                false
            );
            if (nodeData != null)
            {
                nodeData.setConnection(transMgr);

                boolean success = true;
                boolean hasRsc = false;

                rscIterator = nodeData.iterateResources(apiCtx); //accDeniedExc3
                while (rscIterator.hasNext())
                {
                    hasRsc = true;
                    Resource rsc = rscIterator.next();
                    rsc.setConnection(transMgr);
                    rsc.markDeleted(apiCtx); // sqlExc3, accDeniedExc4
                }
                if (!hasRsc)
                {
                    // If the node has no resources, then there should not be any volumes referenced
                    // by the storage pool -- double check
                    storPoolIterator = nodeData.iterateStorPools(apiCtx); // accDeniedExc5
                    while (storPoolIterator.hasNext())
                    {
                        StorPool storPool = storPoolIterator.next();
                        if (storPool.getVolumes(apiCtx).isEmpty())
                        {
                            storPool.setConnection(transMgr);
                            storPool.delete(apiCtx);
                        }
                        else
                        {
                            success = false;
                            ApiCallRcEntry entry = new ApiCallRcEntry();
                            entry.setReturnCode(RC_NODE_DEL_FAIL_EXISTS_VLM);
                            String errorMessage = String.format(
                                "Deletion of node '%s' failed because the storage pool '%s' references volumes " +
                                "on this node, although the node does not reference any resources",
                                nodeNameStr,
                                storPool.getName().displayValue
                            );
                            entry.setMessageFormat(errorMessage);
                            entry.putObjRef(KEY_NODE, nodeNameStr);
                            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                            entry.putVariable(KEY_STOR_POOL_NAME, storPool.getName().displayValue);
                            apiCallRc.addEntry(entry);
                            controller.getErrorReporter().logInfo(errorMessage);
                        }
                    }
                }

                if (success)
                {
                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    String successMessage;
                    if (hasRsc)
                    {
                        nodeData.markDeleted(accCtx); // sqlExc4, accDeniedExc6
                        entry.setReturnCodeBit(RC_NODE_DELETED);
                        successMessage = String.format(
                            "Node '%s' marked for deletion.",
                            nodeNameStr
                        );
                    }
                    else
                    {
                        nodeData.delete(accCtx); // sqlExc5, accDenied7
                        successMessage = String.format(
                            "Node '%s' deleted.",
                            nodeNameStr
                        );
                    }

                    transMgr.commit(); // sqlExc6
                    if (!hasRsc)
                    {
                        controller.nodesMap.remove(nodeData.getName());
                    }

                    entry.setMessageFormat(successMessage);
                    entry.putObjRef(KEY_NODE, nodeNameStr);
                    entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                    apiCallRc.addEntry(entry);
                    controller.getErrorReporter().logInfo(successMessage);

                    // TODO: tell satellites to remove all the corresponding resources and storPools
                    // TODO: if satellites finished, cleanup the storPools and then remove the node from DB
                }
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_NODE_DEL_NOT_FOUND);
                String notFoundMessage = String.format(
                    "Node '%s' was not deleted because it does not exist.",
                    nodeNameStr
                );
                entry.setMessageFormat("Deletion of node '" + currentNodeName.get() + "' had no effect.");
                entry.setCauseFormat("Node '" + currentNodeName.get() + "' does not exist.");
                entry.putObjRef(KEY_NODE, nodeNameStr);
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                apiCallRc.addEntry(entry);
                controller.getErrorReporter().logInfo(notFoundMessage);
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to delete node '%s'.",
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            // handle invalidNameExc1
            String errorMessage = String.format(
                "The node name '%s' is invalid",
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_INVLD_NODE_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            String errorMessage;
            String causeMessage = null;
            String detailsMessage = null;
            Throwable exc;
            errorMessage = "Deletion of node '" + nodeNameStr + "' failed.";
            causeMessage = String.format(
                "Identity '%s' using role '%s' is not authorized to delete node '%s'.",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                nodeNameStr
            );
            causeMessage += "\n";
            causeMessage += accDeniedExc.getMessage();
            exc = accDeniedExc;
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_ACC_DENIED_NODE);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(causeMessage);
            entry.setDetailsFormat(detailsMessage);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    String.format(
                        ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s",
                        nodeNameStr
                    ),
                    dataAlreadyExistsExc
                )
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the node '%s' due to an implementation error.",
                    nodeNameStr
                )
            );
            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting node '%s'.",
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);

            apiCallRc.addEntry(entry);
        }


        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    String errorMessage = String.format(
                        "A database error occured while trying to rollback the deletion of " +
                        "node '%s'.",
                        nodeNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_NODE_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_NODE, nodeNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }

        return apiCallRc;
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
            if (currentApiCallType.get().equals(ApiCallType.CREATE))
            {
                handleAccDeniedExc(
                    accDeniedExc,
                    "create node entries",
                    RC_NODE_CRT_FAIL_ACC_DENIED_NODE
                );
            }
            else
            {
                handleAccDeniedExc(
                    accDeniedExc,
                    "delete node entries",
                    RC_NODE_DEL_FAIL_ACC_DENIED_NODE
                );
            }
            throw new ApiCallHandlerFailedException();
        }
    }

    private TransactionMgr createNewTransMgr() throws ApiCallHandlerFailedException
    {
        try
        {
            return new TransactionMgr(controller.dbConnPool.getConnection());
        }
        catch (SQLException sqlExc)
        {
            String action = "creating a new transaction manager";

            if (currentApiCallType.get().equals(ApiCallType.CREATE))
            {
                handleSqlExc(sqlExc, action, ApiConsts.RC_NODE_CRT_FAIL_SQL);
            }
            else
            {
                handleSqlExc(sqlExc, action, ApiConsts.RC_NODE_DEL_FAIL_SQL);
            }
            throw new ApiCallHandlerFailedException();
        }
    }

    private NodeName asNodeName(String nodeNameStr) throws ApiCallHandlerFailedException
    {
        NodeName nodeName;
        if (currentApiCallType.get().equals(ApiCallType.CREATE))
        {
            nodeName = asNodeName(nodeNameStr, RC_NODE_CRT_FAIL_INVLD_NODE_NAME);
        }
        else
        {
            nodeName = asNodeName(nodeNameStr, RC_NODE_DEL_FAIL_INVLD_NODE_NAME);
        }
        return nodeName;
    }

    private NodeType asNodeType(String nodeTypeStr) throws ApiCallHandlerFailedException
    {
        try
        {
            return NodeType.valueOfIgnoreCase(nodeTypeStr, NodeType.SATELLITE);
        }
        catch (IllegalArgumentException illegalArgExc)
        {
            report(
                illegalArgExc,
                "The specified node type '" + nodeTypeStr + "' is invalid.",
                null, null,
                "Valid node types are:\n" +
                NodeType.CONTROLLER.name() + "\n" +
                NodeType.SATELLITE.name() + "\n" +
                NodeType.COMBINED.name() + "\n" +
                NodeType.AUXILIARY.name() + "\n",
                RC_NODE_CRT_FAIL_INVLD_NODE_TYPE
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    private NodeData createNode(NodeName nodeName, NodeType type, TransactionMgr transMgr)
        throws ApiCallHandlerFailedException
    {
        try
        {
            return NodeData.getInstance(
                currentAccCtx.get(),
                nodeName,
                type,
                new NodeFlag[0],
                transMgr,
                true,
                true
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccDeniedExc(
                accDeniedExc,
                "create the node '" + nodeName + "'.",
                RC_NODE_CRT_FAIL_ACC_DENIED_NODE
            );
            throw new ApiCallHandlerFailedException();
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            report(
                dataAlreadyExistsExc,
                "Creation of node '" + nodeName.displayValue + "' failed.",
                "A node with the specified name '" + nodeName.displayValue + "' already exists.",
                null,
                "- Specify another name for the new node\n" +
                "or\n" +
                "- Delete the existing node before creating a new node with the same name",
                RC_NODE_CRT_FAIL_EXISTS_NODE
            );
            throw new ApiCallHandlerFailedException();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc);
            throw new ApiCallHandlerFailedException();
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
            report(
                invalidNameExc,
                "The specified net interface name '" + netIfNameStr + "' is invalid.",
                RC_NODE_CRT_FAIL_INVLD_NET_NAME
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    private LsIpAddress asDmIpAddress(String ipAddrStr)
        throws ApiCallHandlerFailedException
    {
        if (ipAddrStr == null)
        {
            report(
                null,
                "Node creation failed.",
                "No IP address for the new node was specified",
                null,
                "At least one network interface with a valid IP address must be defined for the new node.",
                RC_NODE_CRT_FAIL_INVLD_NET_ADDR
            );
            throw new ApiCallHandlerFailedException();
        }
        try
        {
            return new LsIpAddress(ipAddrStr);
        }
        catch (InvalidIpAddressException invalidIpExc)
        {
            report(
                invalidIpExc,
                "Node creation failed.",
                "The specified IP address is not valid",
                "The specified input '" + ipAddrStr + "' is not a valid IP address.",
                "Specify a valid IPv4 or IPv6 address.",
                RC_NODE_CRT_FAIL_INVLD_NET_ADDR
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    private void reportMissingNetIfNamespace()
    {
        currentVariables.get().put(KEY_MISSING_NAMESPC, NAMESPC_NETIF);
        report(
            null,
            "Creation of the node '%s' failed.",
            String.format(
                "The path '%s' is not present in the properties specified for the node.\n" +
                "The path contains mandatory parameters required for node creation.",
                NAMESPC_NETIF
            ),
            null,
            "Specify the mandatory parameters.\n" +
            "Example for creating a netinterface named 'netIfName0':\n" +
                NAMESPC_NETIF + "/netIfName0/" + KEY_IP_ADDR + " = 0.0.0.0\n" +
                NAMESPC_NETIF + "/netIfName0/" + KEY_PORT_NR + " = " + DFLT_STLT_PORT_PLAIN + "\n" +
                NAMESPC_NETIF + "/netIfName0/" + KEY_NETCOM_TYPE + " = " + VAL_NETCOM_TYPE_PLAIN + "\n" +
                NAMESPC_NETIF + "/netIfName0/" + KEY_NETCOM_ENABLED + " = " + VAL_TRUE + "\n" +
                "   for a linstor node connection (" + KEY_NETCOM_ENABLED + " is optional and default " + VAL_TRUE+ ") or \n" +
                NAMESPC_NETIF + "/netIfName0/" + KEY_IP_ADDR + " = 10.0.0.103\n" +
                "   for a drbd resource interface (no " + KEY_PORT_NR + ", no " + KEY_NETCOM_TYPE + ")",
            RC_NODE_CRT_FAIL_MISSING_PROPS
        );
        throw new ApiCallHandlerFailedException();
    }

    private void reportMissingPort(String netIfNameStr) throws ApiCallHandlerFailedException
    {
        report(
            null,
            "Creation of the node '" + currentNodeName.get() + " failed.",
            String.format(
                "The mandatory property '%s' for network interface '%s' is unset.",
                KEY_PORT_NR,
                netIfNameStr
            ),
            null,
            null,
            RC_NODE_CRT_FAIL_MISSING_PROP_NETCOM_PORT
        );
        throw new ApiCallHandlerFailedException();
    }

    private void reportMissingNetComType(String netIfNameStr) throws ApiCallHandlerFailedException
    {
        report(
            null,
            "Creation of the node '" + currentNodeName.get() + " failed.",
            String.format(
                "The mandatory property '%s' for the network interface '%s' is unset.",
                KEY_NETCOM_TYPE,
                netIfNameStr
            ),
            null,
            "The mandatory property must be set when requesting node creation.",
            RC_NODE_CRT_FAIL_MISSING_PROP_NETCOM_TYPE
        );
        throw new ApiCallHandlerFailedException();
    }

    private int asPort(String portStr, String netIfNameStr) throws ApiCallHandlerFailedException
    {
        try
        {
            return Integer.parseInt(portStr);
        }
        catch (NumberFormatException numberFormatExc)
        {
            report(
                numberFormatExc,
                "Node creation failed.",
                "The specified TCP/IP port number is invalid",
                "The input specified for the TCP/IP port number field was '" + portStr + "'.",
                "A valid TCP/IP port number must be specified.",
                RC_NODE_CRT_FAIL_INVLD_NET_PORT
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    private NetInterfaceType getNetIfType(
        Props props,
        String netIfNameStr,
        NetInterfaceType defaultType
    )
        throws ApiCallHandlerFailedException
    {
        String netIfTypeStr = null;
        try
        {
            netIfTypeStr = props.getProp(KEY_NETIF_TYPE, netIfNameStr);
            return NetInterfaceType.valueOfIgnoreCase(netIfTypeStr, defaultType);
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            reportImplError(invalidKeyExc);
            throw new ApiCallHandlerFailedException();
        }
        catch (IllegalArgumentException illegalArgumentExc)
        {
            report(
                illegalArgumentExc,
                "The specified network interface type '" + netIfTypeStr + "' for network interface '" +
                netIfNameStr + "' is invalid.",
                RC_NODE_CRT_FAIL_INVLD_NET_TYPE
            );
            throw new ApiCallHandlerFailedException();
        }
    }

    private boolean isEnabled(
        Props props,
        String netIfNameStr,
        boolean defaultValue
    )
        throws ApiCallHandlerFailedException
    {
        try
        {
            boolean retVal = defaultValue;
            String enabledStr = props.getProp(KEY_NETCOM_ENABLED, netIfNameStr);
            if (VAL_TRUE.equalsIgnoreCase(enabledStr))
            {
                retVal = true;
            }
            else
            if (VAL_FALSE.equalsIgnoreCase(enabledStr))
            {
                retVal = false;
            }
            else
            if (enabledStr != null && !enabledStr.trim().equals(""))
            {
                addAnswer(
                    "The node creation request for node '" + currentNodeName.get() + "' was modified automatically.",
                    null,
                    String.format(
                        "Invalid value '%s' for property '%s' was replaced by default value '%s'.",
                        enabledStr,
                        KEY_NETCOM_ENABLED,
                        VAL_TRUE
                    ),
                    "This problem was automatically resolved.",
                    RC_NODE_CRT_WARN_INVLD_OPT_PROP_NETCOM_ENABLED
                );
                // no throw
                retVal = Boolean.parseBoolean(VAL_TRUE);// hopefully VAL_TRUE's value does not change, but still.
            }
            return retVal;
        }
        catch (InvalidKeyException invalidKeyExc)
        {
            reportImplError(invalidKeyExc);
            throw new ApiCallHandlerFailedException();
        }
    }

    private NetInterfaceData createNetInterface(
        Node node,
        NetInterfaceName netName,
        LsIpAddress addr,
        int port,
        NetInterfaceType netType,
        TransactionMgr transMgr
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
                port,
                netType,
                transMgr,
                true,   // persist node
                true    // throw LinStorDataAlreadyExistsException if needed
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            handleAccDeniedExc(
                accDeniedExc,
                "create the netinterface '" + netName + "' on node '" + node.getName() + "'.",
                RC_NODE_CRT_FAIL_ACC_DENIED_NODE
            );
            throw new ApiCallHandlerFailedException();
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            report(
                dataAlreadyExistsExc,
                "Creation of node '" + node.getName() + "' failed.",
                "A duplicate network interface name was encountered during node creation.",
                "The network interface name '" + netName + "' was specified for more than one network interface.",
                "A name that is unique per node must be specified for each network interface.",
                RC_NODE_CRT_FAIL_EXISTS_NET_IF
            );
            throw new ApiCallHandlerFailedException();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc);
            throw new ApiCallHandlerFailedException();
        }
    }

    private void commit(TransactionMgr transMgr) throws ApiCallHandlerFailedException
    {
        try
        {
            transMgr.commit();
        }
        catch (SQLException sqlExc)
        {
            handleSqlExc(sqlExc);
            throw new ApiCallHandlerFailedException();
        }
    }

    private void reportMissingEnabledNetCom() throws ApiCallHandlerFailedException
    {
        report(
            null,
            "Creation of node '" + currentNodeName.get() + "' failed.",
            "No enabled network interface for controller-satellite communication was specified.",
            null,
            "At least one network interface of the node must be enabled for controller-satellite communication.",
            RC_NODE_CRT_FAIL_MISSING_NETCOM
        );
    }

    private void handleSqlExc(SQLException sqlExc)
    {
        if (currentApiCallType.get().equals(ApiCallType.CREATE))
        {
            handleSqlExc(
                sqlExc,
                "creating node '" + currentNodeName.get() + "'",
                RC_NODE_CRT_FAIL_SQL
            );
        }
        else
        {
            handleSqlExc(
                sqlExc,
                "deleting node '" + currentNodeName.get() + "'",
                RC_NODE_DEL_FAIL_SQL
            );
        }
        throw new ApiCallHandlerFailedException();
    }

    private void reportImplError(Throwable throwable)
    {
        if (!(throwable instanceof ImplementationError))
        {
            throwable = new ImplementationError(throwable);
        }

        if (currentApiCallType.get().equals(ApiCallType.CREATE))
        {
            report(
                throwable,
                "The node could not be created due to an implementation error",
                RC_NODE_CRT_FAIL_IMPL_ERROR
            );
        }
        else
        {
            report(
                throwable,
                "The node could not be deleted due to an implementation error",
                RC_NODE_DEL_FAIL_IMPL_ERROR
            );
        }
    }

    private AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer client,
        ApiCallType apiCallType,
        ApiCallRcImpl apiCallRc,
        String nodeNameStr,
        String nodeTypeStr
    )
    {
        super.setCurrent(accCtx, client, apiCallType, apiCallRc);
        currentNodeName.set(nodeNameStr);
        currentNodeType.set(nodeTypeStr);
        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear();
        objRefs.put(KEY_NODE, nodeNameStr);
        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(KEY_NODE, nodeNameStr);
        return this;
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        currentNodeName.set(null);
        currentNodeType.set(null);
    }
}
