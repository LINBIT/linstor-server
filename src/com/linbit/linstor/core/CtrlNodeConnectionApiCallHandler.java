package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

class CtrlNodeConnectionApiCallHandler
{
    private Controller controller;

    CtrlNodeConnectionApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName1 = null;
        NodeName nodeName2 = null;
        NodeData node1 = null;
        NodeData node2 = null;
        NodeConnectionData nodeConn = null;
        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName1 = new NodeName(nodeName1Str);
            nodeName2 = new NodeName(nodeName2Str);

            node1 = NodeData.getInstance( // accDeniedExc1
                accCtx,
                nodeName1,
                null, // nodeFlags only needed if we want to persist this entry
                null, // nodeType only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            node2 = NodeData.getInstance( // accDeniedExc2
                accCtx,
                nodeName2,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            if (node1 == null || node2 == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                String missingNode;
                if (node1 == null)
                {
                    missingNode = nodeName1Str;
                }
                else
                {
                    missingNode = nodeName2Str;
                }

                nodeNotFoundEntry.setReturnCode(RC_NODE_CONN_CRT_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified node '%s' could not be found in the database",
                        missingNode
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, missingNode);
                nodeNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                nodeNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            {
                nodeConn = NodeConnectionData.getInstance( // accDeniedExc3
                    accCtx,
                    node1,
                    node2,
                    transMgr,
                    true, // persist this entry
                    true // throw exception if the entry exists
                );
                nodeConn.setConnection(transMgr);
                nodeConn.getProps(accCtx).map().putAll(nodeConnPropsMap); // accDeniedExc4

                transMgr.commit();

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_NODE_CONN_CREATED);
                String successMessage = String.format(
                    "Node connection between nodes '%s' and '%s' successfully created.",
                    nodeName1Str,
                    nodeName2Str
                );
                entry.setMessageFormat(successMessage);
                entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                entry.putObjRef(KEY_2ND_NODE, nodeName2Str);

                apiCallRc.addEntry(entry);
                controller.getErrorReporter().logInfo(successMessage);
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to create a new node connection between nodes " +
                    "'%s' and '%s'.",
                nodeName1Str,
                nodeName2Str
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            String errorMessage;
            if (nodeName1 == null)
            {
                errorMessage = String.format(
                    "The given node name '%s' is invalid.",
                    nodeName1Str
                );
            }
            else
            {
                errorMessage = String.format(
                    "The given node name '%s' is invalid.",
                    nodeName2Str
                );
            }

            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_INVLD_NODE_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action;
            if (node1 == null || node2 == null)
            { // handle accDeniedExc1 & accDeniedExc2
                String missingNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                action = String.format(
                    "access the node '%s'.",
                    missingNodeNameStr
                );
                entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_ACC_DENIED_NODE);
            }
            else
            { // handle accDeniedExc3, accDeniedExc4
                action = String.format(
                    "create the new node connection between '%s' and '%s'.",
                    nodeName1Str,
                    nodeName2Str
                );
                entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_ACC_DENIED_NODE_CONN);
            }

            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to %s",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (node1 == null || node2 == null)
            {
                String failedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        String.format(
                            ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s",
                            failedNodeNameStr
                        ),
                        dataAlreadyExistsExc
                    )
                );
                entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_IMPL_ERROR);
                errorMessage = String.format(
                    "Failed to create the node connection between the nodes '%s' and '%s' due to an implementation error.",
                    nodeName1Str,
                    nodeName2Str
                );
            }
            else
            {
                entry.setReturnCode(RC_NODE_CONN_CRT_FAIL_EXISTS_NODE_CONN);
                errorMessage = String.format(
                    "A node connection between the two nodes '%s' and '%s' already exists. ",
                    nodeName1Str,
                    nodeName2Str
                );
            }

            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.setMessageFormat(errorMessage);
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while creating a node connection between nodes '%s' and '%s'.",
                nodeName1Str,
                nodeName2Str
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

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
                        "A database error occured while trying to rollback the creation of a connection " +
                            "between the nodes '%s' and '%s'.",
                        nodeName1Str,
                        nodeName2Str
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_NODE_CONN_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
                    entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
                    entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    public ApiCallRc deleteNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName1 = null;
        NodeName nodeName2 = null;
        NodeData node1 = null;
        NodeData node2 = null;
        NodeConnectionData nodeConn = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName1 = new NodeName(nodeName1Str);
            nodeName2 = new NodeName(nodeName2Str);

            node1 = NodeData.getInstance( // accDeniedExc1
                accCtx,
                nodeName1,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            node2 = NodeData.getInstance( // accDeniedExc2
                accCtx,
                nodeName2,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            if (node1 == null || node2 == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                String missingNode;
                if (node1 == null)
                {
                    missingNode = nodeName1Str;
                }
                else
                {
                    missingNode = nodeName2Str;
                }

                nodeNotFoundEntry.setReturnCode(RC_NODE_CONN_DEL_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified node '%s' could not be found in the database",
                        missingNode
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                nodeNotFoundEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                nodeNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                nodeNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            {
                nodeConn = NodeConnectionData.getInstance( // accDeniedExc3
                    accCtx,
                    node1,
                    node2,
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );
                if (nodeConn == null)
                {
                    ApiCallRcEntry nodeConnNotFoundEntry = new ApiCallRcEntry();

                    nodeConnNotFoundEntry.setReturnCode(RC_NODE_CONN_DEL_NOT_FOUND);
                    nodeConnNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified node connection between nodes '%s' and '%s' could not "+
                                "be found in the database",
                            nodeName1Str,
                            nodeName2Str
                        )
                    );
                    nodeConnNotFoundEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                    nodeConnNotFoundEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                    nodeConnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    nodeConnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);

                    apiCallRc.addEntry(nodeConnNotFoundEntry);
                }
                else
                {
                    nodeConn.setConnection(transMgr);
                    nodeConn.delete(accCtx); // accDeniedExc4

                    transMgr.commit();
                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_NODE_CONN_DELETED);
                    String successMessage = String.format(
                        "Node connection between nodes '%s' and '%s' successfully deleted.",
                        nodeName1Str,
                        nodeName2Str
                    );
                    entry.setMessageFormat(successMessage);
                    entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
                    entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
                    entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

                    apiCallRc.addEntry(entry);
                    controller.getErrorReporter().logInfo(successMessage);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to delete node connection between " +
                    "ndoes '%s' and '%s'.",
                nodeName1Str,
                nodeName2Str
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            // handle invalidNameExc1
            String nodeNameInvalidStr = nodeName1 == null ? nodeName1Str : nodeName2Str;
            String errorMessage = String.format(
                "The node name '%s' is invalid",
                nodeNameInvalidStr
            );
            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_INVLD_NODE_NAME);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putVariable(ApiConsts.KEY_NODE_NAME, nodeNameInvalidStr);
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action;
            if (node1 == null || node2 == null)
            { // handle accDeniedExc1 & accDeniedExc2
                String accDeniedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                action = String.format(
                    "access the node '%s'.",
                    accDeniedNodeNameStr
                );
                entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_ACC_DENIED_NODE);
                entry.putVariable(ApiConsts.KEY_NODE_NAME, accDeniedNodeNameStr);
            }
            else
            { // handle accDeniedExc3 & accDeniedExc4
                String subAction;
                if (nodeConn == null)
                {
                    subAction = "access";
                }
                else
                {
                    subAction = "delete";
                }
                action = String.format(
                    "%s the node connection between '%s' and '%s'.",
                    subAction,
                    nodeName1Str,
                    nodeName2Str
                );
                entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_ACC_DENIED_NODE_CONN);
            }

            String errorMessage = String.format(
                "The access context (user: '%s', role: '%s') has no permission to %s" +
                    "create the new node '%s'.",
                accCtx.subjectId.name.displayValue,
                accCtx.subjectRole.name.displayValue,
                action
            );
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(accDeniedExc.getMessage());
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            String implErrorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (node1 == null || node2 == null)
            {
                String failedNodeNameStr;
                failedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s",
                    failedNodeNameStr
                );
                entry.putVariable(ApiConsts.KEY_NODE_NAME, failedNodeNameStr);
            }
            else
            {
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. " +
                        " First NodeName=%s, Second NodeName=%s",
                    nodeName1Str,
                    nodeName2Str
                );
            }

            entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_IMPL_ERROR);
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    implErrorMessage,
                    dataAlreadyExistsExc
                )
            );

            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the node connection between nodes '%s' and '%s' due to an implementation error.",
                    nodeName1Str,
                    nodeName2Str
                )
            );
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting node connection between nodes '%s' and '%s'.",
                nodeName1Str,
                nodeName2Str
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

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
                        "A database error occured while trying to rollback the deletion of a connection " +
                            "between the nodes '%s' and '%s'.",
                        nodeName1Str,
                        nodeName2Str
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_NODE_CONN_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(ApiConsts.KEY_1ST_NODE_NAME, nodeName1Str);
                    entry.putVariable(ApiConsts.KEY_2ND_NODE_NAME, nodeName2Str);
                    entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

}
