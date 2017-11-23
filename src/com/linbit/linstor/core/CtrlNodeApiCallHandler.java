package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.*;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.linstor.DmIpAddress;
import com.linbit.linstor.DrbdDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.NetInterface.NetInterfaceType;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.TcpConnector;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

class CtrlNodeApiCallHandler
{
    private final Controller controller;
    private final AccessContext apiCtx;

    CtrlNodeApiCallHandler(Controller controllerRef, AccessContext apiCtxRef)
    {
        controller = controllerRef;
        apiCtx = apiCtxRef;
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
        String netTypeStr = null;
        String enabledStr = null;
        boolean enabled = true;
        NetInterfaceType netType = null;
        Props netIfProps = null;
        try
        {
            controller.nodesMapProt.requireAccess(accCtx, AccessType.CHANGE);// accDeniedExc1
            transMgr = new TransactionMgr(controller.dbConnPool.getConnection()); // sqlExc1
            nodeName = new NodeName(nodeNameStr); // invalidNameExc1

            type = NodeType.valueOfIgnoreCase(nodeTypeStr, NodeType.SATELLITE);

            NodeFlag[] flags = null;
            node = NodeData.getInstance( // sqlExc2, accDeniedExc2, alreadyExists1
                accCtx,
                nodeName,
                type,
                flags,
                transMgr,
                true,
                true
            );
            node.setConnection(transMgr);

            nodeProps = node.getProps(accCtx); // accDeniedExc0 (implError)
            nodeProps.map().putAll(propsMap);

            netIfProps = nodeProps.getNamespace(NAMESPC_NETIF);
            if (netIfProps == null)
            {
                apiCallRc.addEntry(
                    getApiCallRcMissingNetIfNamespace(
                        nodeNameStr
                    )
                );
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
                    netTypeStr = null;
                    netType = null;
                    enabledStr = null;
                    enabled = true;

                    NetInterfaceName netName = new NetInterfaceName(currentNetIfNameStr); // invalidnameExc2
                    String ipStr = netIfProps.getProp(KEY_IP_ADDR, currentNetIfNameStr);
                    DmIpAddress addr = new DmIpAddress(ipStr);
                    portStr = netIfProps.getProp(KEY_PORT_NR, currentNetIfNameStr);
                    netComTypeStr = netIfProps.getProp(KEY_NETCOM_TYPE, currentNetIfNameStr);
                    if (portStr == null || netComTypeStr == null)
                    {
                        if (portStr == null && netComTypeStr != null)
                        {
                            apiCallRc.addEntry(
                                getApiCallRcMissingPropNetComType(
                                    nodeNameStr,
                                    currentNetIfNameStr
                                )
                            );
                        }
                        else
                        if (portStr != null && netComTypeStr == null)
                        {
                            apiCallRc.addEntry(
                                getApiCallRcMissingPropNetComPort(
                                    nodeNameStr,
                                    currentNetIfNameStr
                                )
                            );
                        }
                    }
                    else
                    {
                        port = Integer.parseInt(portStr);
                        netTypeStr = netIfProps.getProp(KEY_NETIF_TYPE, currentNetIfNameStr);
                        netType = NetInterfaceType.valueOfIgnoreCase(netTypeStr, NetInterfaceType.IP);
                        enabledStr = netIfProps.getProp(KEY_NETCOM_ENABLED, currentNetIfNameStr);
                        if (VAL_FALSE.equalsIgnoreCase(enabledStr) || VAL_TRUE.equalsIgnoreCase(enabledStr))
                        {
                            enabled = Boolean.parseBoolean(enabledStr);
                        }
                        else
                        {
                            enabled = true;
                            if (enabledStr != null && !enabledStr.trim().equals(""))
                            {
                                apiCallRc.addEntry(
                                    getApiCallRcWarnInvalidNetComEnabled(
                                        nodeNameStr,
                                        enabledStr
                                    )
                                );
                            }
                        }

                        if (enabled)
                        {
                            atLeastOneEnabled = true;
                            NetInterfaceData.getInstance( // sqlExc3, accDeniedExc4, alreadyExists2
                                accCtx,
                                node,
                                netName,
                                addr,
                                port,
                                netType,
                                transMgr,
                                true,
                                true
                            );
                        }
                    }
                }

                if (atLeastOneEnabled == false)
                {
                    apiCallRc.addEntry(
                        getApiCallRcMissingEnabledNetCom(
                            nodeNameStr
                        )
                    );
                }
                else
                {
                    transMgr.commit(); // sqlExc4

                    String successMessage = String.format(
                        "Node '%s' successfully created.",
                        nodeNameStr
                    );
                    apiCallRc.addEntry(
                        getApiCallRcNodeCreated(
                            nodeNameStr,
                            successMessage
                        )
                    );
                    controller.nodesMap.put(nodeName, node);
                    controller.getErrorReporter().logInfo(successMessage);

                    startConnecting(node, accCtx, client);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to create a new node '%s'.",
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (nodeName == null)
            { // handle invalidNameExc1
                errorMessage = String.format(
                    "The given node name '%s' is invalid.",
                    nodeNameStr
                );

                entry.setReturnCodeBit(RC_NODE_CRT_FAIL_INVLD_NODE_NAME);
            }
            else
            { // handle invalidNameExc2
                errorMessage = String.format(
                    "The given network interface name '%s' is invalid",
                    currentNetIfNameStr
                );
                entry.setReturnCodeBit(RC_NODE_CRT_FAIL_INVLD_NET_NAME);
                entry.putVariable(KEY_NET_IF_NAME, currentNetIfNameStr);
            }
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            Throwable exc;
            String errorMessage;
            if (node == null)
            { // handle accDeniedExc1 && accDeniedExc2
                errorMessage = String.format(
                    "The access context (user: '%s', role: '%s') has no permission to " +
                        "create the new node '%s'.",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    nodeNameStr
                );
                entry.setReturnCodeBit(RC_NODE_CRT_FAIL_ACC_DENIED_NODE);
                exc = accDeniedExc;
            }
            else
            { // handle accDeniedExc3 && accDeniedExc4
                errorMessage = String.format(
                    "The node '%s' could not be created due to an implementation error",
                    nodeNameStr
                );
                entry.setReturnCode(RC_NODE_CRT_FAIL_IMPL_ERROR);
                exc = new ImplementationError(errorMessage, accDeniedExc);
            }
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException alreadyExistsExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            Throwable exc;
            if (node == null)
            { // handle alreadyExists1
                errorMessage = String.format(
                    "A node with the name '%s' already exists",
                    nodeNameStr
                );
                entry.setReturnCodeBit(RC_NODE_CRT_FAIL_EXISTS_NODE);
                exc = alreadyExistsExc;
            }
            else
            {
                errorMessage = String.format(
                    "The node '%s' could not be created due to an implementation error",
                    nodeNameStr
                );
                entry.setReturnCode(RC_NODE_CRT_FAIL_IMPL_ERROR);
                exc = new ImplementationError(errorMessage, alreadyExistsExc);
            }
            controller.getErrorReporter().reportError(
                alreadyExistsExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(alreadyExistsExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (IllegalArgumentException illegalArgExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (type == null)
            {
                errorMessage = String.format("Unknown node type '%s'.", nodeTypeStr);
                entry.setReturnCodeBit(RC_NODE_CRT_FAIL_INVLD_NODE_TYPE);
            }
            else
            if (port == null)
            {
                errorMessage = String.format(
                    "Invalid port '%s' for net name '%s'.",
                    portStr,
                    currentNetIfNameStr
                );
                entry.setReturnCode(RC_NODE_CRT_FAIL_INVLD_NET_PORT);
            }
            else
            {
                errorMessage = String.format(
                    "Invalid network interface type '%s' for net name '%s'.",
                    netTypeStr,
                    currentNetIfNameStr
                );
                entry.setReturnCode(RC_NODE_CRT_FAIL_INVLD_NET_TYPE);
            }
            controller.getErrorReporter().reportError(
                illegalArgExc,
                accCtx,
                client,
                errorMessage
            );
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(illegalArgExc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_NODE_TYPE, nodeTypeStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            String errorMessage = String.format(
                "An unknown exception occured while creating node '%s'.",
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CRT_FAIL_UNKNOWN_ERROR);
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
                            controller.getErrorReporter().reportError(
                                new LinStorException(
                                    "The controller has no default " + type.toLowerCase() + " tcp connector "
                                ),
                                accCtx,
                                client,
                                "Controller tried to establish connection to other node, but default tcp connector is missing"
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
                        new ImplementationError(
                            "Default tcp connector peer has not a valid ServiceName",
                            invalidNameExc
                        )
                    );
                }
                catch (InvalidKeyException invalidKeyExc)
                {
                    controller.getErrorReporter().reportError(
                        new ImplementationError(
                            "Hardcoded props key threw InvalidKeyException",
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
                    "An already validated access suddenly has no more access", exc
                )
            );
        }
    }

    private ApiCallRcEntry getApiCallRcMissingNetIfNamespace(String nodeNameStr)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(RC_NODE_CRT_FAIL_MISSING_PROPS);

        String errorMessage = String.format(
            "Node '%s' is missing the props-namespace '%s'.",
            nodeNameStr,
            NAMESPC_NETIF
        );
        entry.setMessageFormat(errorMessage);
        entry.setCorrectionFormat("Example for creating a netinterface named 'netIfName0': \n" +
            NAMESPC_NETIF + "/netIfName0/" + KEY_IP_ADDR + " = 0.0.0.0\n" +
            NAMESPC_NETIF + "/netIfName0/" + KEY_PORT_NR + " = " + DFLT_STLT_PORT_PLAIN + "\n" +
            NAMESPC_NETIF + "/netIfName0/" + KEY_NETCOM_TYPE + " = " + VAL_NETCOM_TYPE_PLAIN + "\n" +
            NAMESPC_NETIF + "/netIfName0/" + KEY_NETCOM_ENABLED + " = " + VAL_TRUE + "\n" +
            "   for a linstor node connection (" + KEY_NETCOM_ENABLED + " is optional and default " + VAL_TRUE+ ") or \n" +
            NAMESPC_NETIF + "/netIfName0/" + KEY_IP_ADDR + " = 10.0.0.103\n" +
            "   for a drbd resource interface (no " + KEY_PORT_NR + ", no " + KEY_NETCOM_TYPE + ")"
        );
        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
        entry.putVariable(KEY_MISSING_NAMESPC, NAMESPC_NETCOM);
        entry.putObjRef(KEY_NODE, nodeNameStr);
        return entry;
    }

    private ApiCallRcEntry getApiCallRcNodeCreated(String nodeNameStr, String successMessage)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(RC_NODE_CREATED);
        entry.setMessageFormat(successMessage);
        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
        entry.putObjRef(KEY_NODE, nodeNameStr);
        return entry;
    }

    private ApiCallRcEntry getApiCallRcMissingEnabledNetCom(String nodeNameStr)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(RC_NODE_CRT_FAIL_MISSING_NETCOM);

        String errorMessage = String.format(
            "No (enabled) network interface for linstor communication was specified for node '%s'.",
            nodeNameStr
        );
        entry.setMessageFormat(errorMessage);
        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
        entry.putObjRef(KEY_NODE, nodeNameStr);
        return entry;
    }

    private ApiCallRcEntry getApiCallRcWarnInvalidNetComEnabled(String nodeNameStr, String enabledStr)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(RC_NODE_CRT_WARN_INVLD_OPT_PROP_NETCOM_ENABLED);

        String errorMessage = String.format(
            "The property '%s' for node '%s' was '%s', which is invalid. The property defaults to '%s'.",
            KEY_NETCOM_ENABLED,
            nodeNameStr,
            enabledStr,
            VAL_TRUE
        );
        entry.setMessageFormat(errorMessage);
        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
        entry.putVariable(KEY_NETCOM_ENABLED, enabledStr);
        entry.putObjRef(KEY_NODE, nodeNameStr);
        return entry;
    }

    private ApiCallRcEntry getApiCallRcMissingPropNetComPort(String nodeNameStr, String currentNetIfNameStr)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(RC_NODE_CRT_FAIL_MISSING_PROP_NETCOM_PORT);

        String errorMessage = String.format(
            "The property '%s' for node '%s' for network interface '%s' is missing.",
            KEY_PORT_NR,
            nodeNameStr,
            currentNetIfNameStr
        );
        entry.setMessageFormat(errorMessage);
        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
        entry.putVariable(KEY_PORT_NR, "");
        entry.putObjRef(KEY_NODE, nodeNameStr);
        return entry;
    }

    private ApiCallRcEntry getApiCallRcMissingPropNetComType(String nodeNameStr, String currentNetIfNameStr)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();
        entry.setReturnCodeBit(RC_NODE_CRT_FAIL_MISSING_PROP_NETCOM_TYPE);

        String errorMessage = String.format(
            "The property '%s' for node '%s' for network interface '%s' is missing.",
            KEY_NETCOM_TYPE,
            nodeNameStr,
            currentNetIfNameStr
        );
        entry.setMessageFormat(errorMessage);
        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
        entry.putVariable(KEY_NETCOM_TYPE, "");
        entry.putObjRef(KEY_NODE, nodeNameStr);
        return entry;
    }

    ApiCallRc deleteNode(AccessContext accCtx, Peer client, String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;
        NodeData nodeData = null;
        Iterator<Resource> rscIterator = null;

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
                nodeData.markDeleted(accCtx); // sqlExc3, accDeniedExc3

                rscIterator = nodeData.iterateResources(apiCtx); //accDeniedExc4
                while (rscIterator.hasNext())
                {
                    Resource rsc = rscIterator.next();
                    rsc.setConnection(transMgr);
                    rsc.markDeleted(apiCtx); // sqlExc4, accDeniedExc4
                }

                transMgr.commit(); // sqlExc5

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_NODE_DELETED);
                String successMessage = String.format(
                    "Node '%s' marked to be deleted.",
                    nodeNameStr
                );
                entry.setMessageFormat(successMessage);
                entry.putObjRef(KEY_NODE, nodeNameStr);
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                apiCallRc.addEntry(entry);
                controller.getErrorReporter().logInfo(successMessage);

                // TODO: tell satellites to remove all the corresponding resources
                // TODO: if satellites finished, cleanup the storPools and then remove the node from DB
            }
            else
            {
                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_NODE_DEL_NOT_FOUND);
                String notFoundMessage = String.format(
                    "Node '%s' was not deleted as it was not found",
                    nodeNameStr
                );
                entry.setMessageFormat(notFoundMessage);
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
            Throwable exc;
            if (rscIterator == null)
            { // handle accDeniedExc1 && accDeniedExc2 && accDeniedExc3
                errorMessage = String.format(
                    "The access context (user: '%s', role: '%s') has no permission to " +
                        "delete the node '%s'.",
                    accCtx.subjectId.name.displayValue,
                    accCtx.subjectRole.name.displayValue,
                    nodeNameStr
                );
                exc = accDeniedExc;
            }
            else
            { // handle accDeniedExc4 && accDeniedExc5
                errorMessage = String.format(
                    "The resources deployed on node '%s' could not be marked for "+
                        "deletion due to an implementation error.",
                    nodeNameStr
                );
                exc = new ImplementationError(
                    "ApiContext does not haven sufficent permission to mark resources as deleted",
                    accDeniedExc
                );
            }
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_ACC_DENIED_NODE);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    String.format(
                        ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s",
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

}
