package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceConnectionData;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

class CtrlRscConnectionApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName1 = new ThreadLocal<>();
    private final ThreadLocal<String> currentNodeName2 = new ThreadLocal<>();
    private final ThreadLocal<String> currentRscName = new ThreadLocal<>();

    CtrlRscConnectionApiCallHandler(Controller controllerRef)
    {
        super (
            controllerRef,
            null, // apiCtx,
            ApiConsts.MASK_RSC_CONN
        );
        super.setNullOnAutoClose(
            currentNodeName1,
            currentNodeName2,
            currentRscName
        );
    }

    public ApiCallRc createResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        Map<String, String> rscConnPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName1 = null;
        NodeName nodeName2 = null;
        ResourceName rscName = null;
        NodeData node1 = null;
        NodeData node2 = null;
        ResourceDefinitionData rscDfn = null;
        ResourceData rsc1 = null;
        ResourceData rsc2 = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName1 = new NodeName(nodeName1Str);
            nodeName2 = new NodeName(nodeName2Str);
            rscName = new ResourceName(rscNameStr);

            node1 = NodeData.getInstance( // accDeniedExc1
                accCtx,
                nodeName1,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this object
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            node2 = NodeData.getInstance( // accDeniedExc2
                accCtx,
                nodeName2,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this object
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            rscDfn = ResourceDefinitionData.getInstance(
                accCtx,
                rscName,
                null, // port only needed if we want to persist this entry
                null, // rscFalgs only needed if we want to persist this object
                null, // secret only needed if we want to persist this object
                null, // transportType only needed if we want to persist this object
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

                nodeNotFoundEntry.setReturnCode(RC_RSC_CONN_CRT_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified node '%s' could not be found in the database",
                        missingNode
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, missingNode);
                nodeNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                nodeNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                nodeNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            if (rscDfn == null)
            {
                ApiCallRcEntry rscDfnNotFoundEntry = new ApiCallRcEntry();
                rscDfnNotFoundEntry.setReturnCode(RC_RSC_CONN_CRT_FAIL_NOT_FOUND_RSC_DFN);
                rscDfnNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified resource definition '%s' could not be found in the database",
                        rscNameStr
                    )
                );
                rscDfnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                rscDfnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                rscDfnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                rscDfnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(rscDfnNotFoundEntry);
            }
            else
            {
                rsc1 = ResourceData.getInstance(
                    accCtx,
                    rscDfn,
                    node1,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );
                rsc2 = ResourceData.getInstance(
                    accCtx,
                    rscDfn,
                    node2,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );

                if (rsc1 == null || rsc2 == null)
                {
                    String missingRscNode;
                    if (rsc1 == null)
                    {
                        missingRscNode = nodeName1Str;
                    }
                    else
                    {
                        missingRscNode = nodeName2Str;
                    }

                    ApiCallRcEntry rscNotFoundEntry = new ApiCallRcEntry();
                    rscNotFoundEntry.setReturnCode(RC_RSC_CONN_CRT_FAIL_NOT_FOUND_RSC);
                    rscNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified resource '%s' on node '%s' could not be found in the database",
                            rscNameStr,
                            missingRscNode
                        )
                    );
                    rscNotFoundEntry.putVariable(KEY_NODE_NAME, missingRscNode);
                    rscNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    rscNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    rscNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    rscNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(rscNotFoundEntry);
                }
                else
                {
                    ResourceConnectionData rscConn = ResourceConnectionData.getInstance( // accDeniedExc3
                        accCtx,
                        rsc1,
                        rsc2,
                        transMgr,
                        true, // persist this entry
                        true // throw exception if the entry exists
                    );
                    rscConn.setConnection(transMgr);
                    rscConn.getProps(accCtx).map().putAll(rscConnPropsMap);

                    transMgr.commit();

                    ApiCallRcEntry successEntry = new ApiCallRcEntry();
                    successEntry.setReturnCodeBit(RC_RSC_CONN_CREATED);
                    String successMessage = String.format(
                        "Resource connection between nodes '%s' and '%s' on resource '%s' successfully created.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr
                    );
                    successEntry.setMessageFormat(successMessage);
                    successEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                    successEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                    successEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    successEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    successEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    successEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(successEntry);
                    controller.getErrorReporter().logInfo(successMessage);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to create a new resource connection between nodes " +
                    "'%s' and '%s' on resource '%s'.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (nodeName1 == null || nodeName2 == null)
            {
                String invalidNodeName = nodeName1 == null ? nodeName1Str : nodeName2Str;
                errorMessage = String.format(
                    "The given node name '%s' is invalid.",
                    invalidNodeName
                );
                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_INVLD_NODE_NAME);
                entry.putVariable(KEY_NODE_NAME, invalidNodeName);
            }
            else
            {
                errorMessage = String.format(
                    "The given resource name '%s' is invalid.",
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_INVLD_RSC_NAME);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }

            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

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

                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_ACC_DENIED_NODE);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
            }
            else
            if (rscDfn == null)
            { // handle accDeniedExc3
                action = String.format(
                    "access the resource definition '%s'.",
                    rscNameStr
                );

                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_ACC_DENIED_RSC_DFN);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }
            else
            if (rsc1 == null || rsc2 == null)
            { // handle accDeniedExc4 & accDeniedExc5
                String accDeniedNodeNameStr = rsc1 == null ? nodeName1Str : nodeName2Str;
                action = String.format(
                    "access the resource '%s' on node '%s'.",
                    rscNameStr,
                    accDeniedNodeNameStr
                );
                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_ACC_DENIED_RSC);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }
            else
            { // handle accDeniedExc6
                action = String.format(
                    "create the resource connection between nodes '%s' and '%s' on resource '%s'.",
                    nodeName1Str,
                    nodeName2Str,
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_ACC_DENIED_RSC_CONN);
                entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
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
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (node1 == null || node2 == null || rscDfn == null || rsc1 == null || rsc2 == null)
            {
                String implErrorMessage;
                if (node1 == null || node2 == null)
                {
                    String failedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s",
                        failedNodeNameStr
                    );
                }
                else
                if (rscDfn == null)
                {
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. RscName=%s",
                        rscNameStr
                    );
                }
                else
                {
                    String failedNodeNameStr = rsc1 == null ? nodeName1Str : nodeName2Str;
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s, ResName=%s",
                        failedNodeNameStr,
                        rscNameStr
                    );
                }
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        implErrorMessage,
                        dataAlreadyExistsExc
                    )
                );
                entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_IMPL_ERROR);
                errorMessage = String.format(
                    "Failed to create the resource connection between the nodes '%s' and '%s' on resource " +
                        "'%s' due to an implementation error.",
                    nodeName1Str,
                    nodeName2Str,
                    rscNameStr
                );
            }
            else
            {
                entry.setReturnCode(RC_RSC_CONN_CRT_FAIL_EXISTS_RSC_CONN);
                errorMessage = String.format(
                    "A resource connection between the two nodes '%s' and '%s' on resource '%s' " +
                        "already exists. ",
                    nodeName1Str,
                    nodeName2Str,
                    rscNameStr
                );
            }

            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.setMessageFormat(errorMessage);
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while creating resource connection between nodes '%s' " +
                    " and '%s' for resource '%s'.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

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
                        "A database error occured while trying to rollback the creation of a resource connection " +
                            "between the nodes '%s' and '%s' on resource '%s'.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_CONN_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                    entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                    entry.putVariable(KEY_RSC_NAME, rscNameStr);
                    entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    public ApiCallRc modifyRscConnection(
        AccessContext accCtx,
        Peer client,
        UUID rscConnUuid,
        String nodeName1,
        String nodeName2,
        String rscNameStr,
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
                null, // new transMgr
                nodeName1,
                nodeName2,
                rscNameStr
            );
        )
        {
            ResourceConnectionData rscConn = loadRscConn(nodeName1, nodeName2, rscNameStr);

            if (rscConnUuid != null && !rscConnUuid.equals(rscConn.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_RSC_CONN
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(rscConn);
            Map<String, String> propsMap = props.map();

            propsMap.putAll(overrideProps);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            // TODO update satellites
            reportSuccess(getObjectDescription() + " updated.");
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // failure was reported and added to returning apiCallRc
            // this is only for flow-control.
        }

        return apiCallRc;
    }

    public ApiCallRc deleteResourceConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName1 = null;
        NodeName nodeName2 = null;
        ResourceName rscName = null;
        NodeData node1 = null;
        NodeData node2 = null;
        ResourceDefinitionData rscDfn = null;
        ResourceData rsc1 = null;
        ResourceData rsc2 = null;

        ResourceConnectionData rscConn = null;


        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName1 = new NodeName(nodeName1Str);
            nodeName2 = new NodeName(nodeName2Str);
            rscName = new ResourceName(rscNameStr);

            node1 = NodeData.getInstance( // accDeniedExc1
                accCtx,
                nodeName1,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this object
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            node2 = NodeData.getInstance( // accDeniedExc2
                accCtx,
                nodeName2,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this object
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            rscDfn = ResourceDefinitionData.getInstance(
                accCtx,
                rscName,
                null, // port only needed if we want to persist this entry
                null, // rscFalgs only needed if we want to persist this object
                null, // secret only needed if we want to persist this object
                null, // transportType only needed if we want to persist this object
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

                nodeNotFoundEntry.setReturnCode(RC_RSC_CONN_DEL_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified node '%s' could not be found in the database",
                        missingNode
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, missingNode);
                nodeNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                nodeNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                nodeNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            if (rscDfn == null)
            {
                ApiCallRcEntry rscDfnNotFoundEntry = new ApiCallRcEntry();
                rscDfnNotFoundEntry.setReturnCode(RC_RSC_CONN_DEL_FAIL_NOT_FOUND_RSC_DFN);
                rscDfnNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified resource definition '%s' could not be found in the database",
                        rscNameStr
                    )
                );
                rscDfnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                rscDfnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                rscDfnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                rscDfnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(rscDfnNotFoundEntry);
            }
            else
            {
                rsc1 = ResourceData.getInstance(
                    accCtx,
                    rscDfn,
                    node1,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );
                rsc2 = ResourceData.getInstance(
                    accCtx,
                    rscDfn,
                    node2,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );

                if (rsc1 == null || rsc2 == null)
                {
                    String missingRscNode;
                    if (rsc1 == null)
                    {
                        missingRscNode = nodeName1Str;
                    }
                    else
                    {
                        missingRscNode = nodeName2Str;
                    }

                    ApiCallRcEntry rscNotFoundEntry = new ApiCallRcEntry();
                    rscNotFoundEntry.setReturnCode(RC_RSC_CONN_DEL_FAIL_NOT_FOUND_RSC);
                    rscNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified resource '%s' on node '%s' could not be found in the database",
                            rscNameStr,
                            missingRscNode
                        )
                    );
                    rscNotFoundEntry.putVariable(KEY_NODE_NAME, missingRscNode);
                    rscNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    rscNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    rscNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    rscNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(rscNotFoundEntry);
                }
                else
                {
                    rscConn = ResourceConnectionData.getInstance( // accDeniedExc3
                        accCtx,
                        rsc1,
                        rsc2,
                        transMgr,
                        false, // do not persist this entry
                        false // do not throw exception if the entry exists
                    );
                    if (rscConn == null)
                    {
                        ApiCallRcEntry rscConnNotFoundEntry = new ApiCallRcEntry();

                        rscConnNotFoundEntry.setReturnCode(RC_RSC_CONN_DEL_WARN_NOT_FOUND);
                        rscConnNotFoundEntry.setCauseFormat(
                            String.format(
                                "The specified resource connection between nodes '%s' and '%s' on resource '%s' "+
                                    "could not be found in the database",
                                nodeName1Str,
                                nodeName2Str,
                                rscNameStr
                            )
                        );
                        rscConnNotFoundEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                        rscConnNotFoundEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                        rscConnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                        rscConnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                        rscConnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                        rscConnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                        apiCallRc.addEntry(rscConnNotFoundEntry);
                    }
                    else
                    {
                        rscConn.setConnection(transMgr);
                        rscConn.delete(accCtx);

                        transMgr.commit();

                        ApiCallRcEntry successEntry = new ApiCallRcEntry();
                        successEntry.setReturnCodeBit(RC_RSC_CONN_DELETED);
                        String successMessage = String.format(
                            "Resource connection between nodes '%s' and '%s' on resource '%s' successfully deleted.",
                            nodeName1Str,
                            nodeName2Str,
                            rscNameStr
                        );
                        successEntry.setMessageFormat(successMessage);
                        successEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                        successEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                        successEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                        successEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                        successEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                        successEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                        apiCallRc.addEntry(successEntry);
                        controller.getErrorReporter().logInfo(successMessage);
                    }
                }
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to delete resource connection between " +
                    "ndoes '%s' and '%s' on resource '%s'.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (nodeName1 == null || nodeName2 == null)
            {
                String invalidNodeName = nodeName1 == null ? nodeName1Str : nodeName2Str;
                errorMessage = String.format(
                    "The given node name '%s' is invalid.",
                    invalidNodeName
                );
                entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_INVLD_NODE_NAME);
                entry.putVariable(KEY_NODE_NAME, invalidNodeName);
            }
            else
            {
                errorMessage = String.format(
                    "The given resource name '%s' is invalid.",
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_INVLD_RSC_NAME);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }

            controller.getErrorReporter().reportError(
                invalidNameExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(invalidNameExc.getMessage());

            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

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

                entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_ACC_DENIED_NODE);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
            }
            else
            if (rscDfn == null)
            { // handle accDeniedExc3
                action = String.format(
                    "access the resource definition '%s'.",
                    rscNameStr
                );

                entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_ACC_DENIED_RSC_DFN);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }
            else
            if (rsc1 == null || rsc2 == null)
            { // handle accDeniedExc4 & accDeniedExc5
                String accDeniedNodeNameStr = rsc1 == null ? nodeName1Str : nodeName2Str;
                action = String.format(
                    "access the resource '%s' on node '%s'.",
                    rscNameStr,
                    accDeniedNodeNameStr
                );
                entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_ACC_DENIED_RSC);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }
            else
            {
                if (rscConn == null)
                { // handle accDeniedExc6
                    action = String.format(
                        "access the resource connection between nodes '%s' and '%s' on resource '%s'.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr
                    );
                    entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_ACC_DENIED_RSC_CONN);
                }
                else
                { // handle accDeniedExc7
                    action = String.format(
                        "access the resource connection between nodes '%s' and '%s' on resource '%s'.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr
                    );
                }
                entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
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
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String implErrorMessage;
            if (node1 == null || node2 == null)
            {
                String failedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s",
                    failedNodeNameStr
                );
            }
            else
            if (rscDfn == null)
            {
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. RscName=%s",
                    rscNameStr
                );
            }
            else
            {
                String failedNodeNameStr = rsc1 == null ? nodeName1Str : nodeName2Str;
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an LinStorDataAlreadyExistsException. NodeName=%s, ResName=%s",
                    failedNodeNameStr,
                    rscNameStr
                );
            }
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    implErrorMessage,
                    dataAlreadyExistsExc
                )
            );
            entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_IMPL_ERROR);
            errorMessage = String.format(
                "Failed to delete the resource connection between the nodes '%s' and '%s' on resource " +
                    "'%s' due to an implementation error.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr
            );

            entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.setMessageFormat(errorMessage);
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting resource connection between nodes '%s' " +
                    " and '%s' for resource '%s'.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

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
                        "A database error occured while trying to rollback the deletion of a resource connection " +
                            "between the nodes '%s' and '%s' on resource '%s'.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_CONN_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                    entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                    entry.putVariable(KEY_RSC_NAME, rscNameStr);
                    entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr);
        }
        return apiCallRc;
    }

    private AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeName1,
        String nodeName2,
        String rscNameStr
    )
    {
        super.setCurrent(accCtx, peer, type, apiCallRc, transMgr);

        currentNodeName1.set(nodeName1);
        currentNodeName2.set(nodeName2);
        currentRscName.set(rscNameStr);

        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear();
        objRefs.put(ApiConsts.KEY_1ST_NODE, nodeName1);
        objRefs.put(ApiConsts.KEY_2ND_NODE, nodeName2);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(ApiConsts.KEY_1ST_NODE_NAME, nodeName1);
        vars.put(ApiConsts.KEY_2ND_NODE_NAME, nodeName2);
        vars.put(ApiConsts.KEY_RSC_NAME, rscNameStr);

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Resource connection between nodes " + currentNodeName1.get() + " and " +
            currentNodeName2.get() + " for resource " + currentRscName.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return "resource connection between nodes '" + currentNodeName1.get() + "' and '" +
            currentNodeName2.get() + "' for resource '" + currentRscName.get() + "'";
    }

    private ResourceConnectionData loadRscConn(
        String nodeName1,
        String nodeName2,
        String rscNameStr
    )
        throws ApiCallHandlerFailedException
    {
        NodeData node1 = loadNode(nodeName1);
        NodeData node2 = loadNode(nodeName2);
        ResourceName rscName = asRscName(rscNameStr);

        Resource rsc1 = loadRsc(node1, rscName);
        Resource rsc2 = loadRsc(node2, rscName);

        try
        {
            return ResourceConnectionData.getInstance(
                currentAccCtx.get(),
                rsc1,
                rsc2,
                currentTransMgr.get(),
                false,
                false
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading " + getObjectDescription() + ".",
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asImplError(dataAlreadyExistsExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading " + getObjectDescription()
            );
        }
    }

    private Resource loadRsc(NodeData node, ResourceName rscName) throws ApiCallHandlerFailedException
    {
        try
        {
            return node.getResource(
                currentAccCtx.get(),
                rscName
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading resource '" + rscName.displayValue + "' from node '" + node.getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
    }

    private Props getProps(ResourceConnectionData rscConn) throws ApiCallHandlerFailedException
    {
        try
        {
            return rscConn.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "accessing properties of " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_RSC_CONN
            );
        }
    }
}
