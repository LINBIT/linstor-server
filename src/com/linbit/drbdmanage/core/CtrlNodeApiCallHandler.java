package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.ApiCallRcConstants.*;
import static com.linbit.drbdmanage.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.NetInterface.NetInterfaceType;
import com.linbit.drbdmanage.NetInterfaceData;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.DmIpAddress;
import com.linbit.drbdmanage.Node.NodeFlag;
import com.linbit.drbdmanage.Node.NodeType;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

class CtrlNodeApiCallHandler
{
    private final Controller controller;

    CtrlNodeApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createNode(
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
        String portStr = null;
        Integer port = null;
        String netTypeStr = null;
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
            Set<String> netIfNames = netIfProps.keySet();
            for (String netIfNameStr : netIfNames)
            {
                currentNetIfNameStr = netIfNameStr;
                portStr = null;
                port = null;
                netTypeStr = null;
                netType = null;

                NetInterfaceName netName = new NetInterfaceName(netIfNameStr); // invalidnameExc2
                DmIpAddress addr = new DmIpAddress(
                    netIfProps.getProp(
                        KEY_IP_ADDR,
                        netIfNameStr
                    )
                );
                portStr = netIfProps.getProp(
                    KEY_PORT_NR,
                    netIfNameStr
                );
                port = Integer.parseInt(portStr);
                netTypeStr = netIfProps.getProp(
                    KEY_NETIF_TYPE,
                    netIfNameStr
                );
                netType = NetInterfaceType.valueOfIgnoreCase(
                    netTypeStr,
                    NetInterfaceType.IP
                );
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

            transMgr.commit(); // sqlExc4

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CREATED);
            String successMessage = String.format(
                "Node '%s' successfully created.",
                nodeNameStr
            );
            entry.setMessageFormat(successMessage);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);

            apiCallRc.addEntry(entry);
            controller.nodesMap.put(nodeName, node);
            controller.getErrorReporter().logInfo(successMessage);
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

    public ApiCallRc deleteNode(AccessContext accCtx, Peer client, String nodeNameStr)
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;
        NodeData nodeData = null;

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
                transMgr.commit(); // sqlExc4

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
                // TODO: if satellite is finished remove the node from the DB
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
            // handle accDeniedExc1 && accDeniedExc2 && accDeniedExc3
            String errormessage = String.format(
                "The given access context has no permission to delete node '%s'.",
                nodeNameStr
            );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errormessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_DEL_FAIL_ACC_DENIED_NODE);
            entry.setMessageFormat(errormessage);
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
