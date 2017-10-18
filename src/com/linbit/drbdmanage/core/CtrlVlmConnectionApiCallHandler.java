package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.ApiCallRcConstants.*;
import static com.linbit.drbdmanage.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.VolumeConnectionData;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinitionData;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

class CtrlVlmConnectionApiCallHandler
{
    private Controller controller;

    CtrlVlmConnectionApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt,
        Map<String, String> vlmConnPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName1 = null;
        NodeName nodeName2 = null;
        ResourceName rscName = null;
        VolumeNumber vlmNr = null;
        NodeData node1 = null;
        NodeData node2 = null;
        ResourceDefinitionData rscDfn = null;
        ResourceData rsc1 = null;
        ResourceData rsc2 = null;
        VolumeDefinitionData vlmDfn = null;
        VolumeData vlm1 = null;
        VolumeData vlm2 = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName1 = new NodeName(nodeName1Str);
            nodeName2 = new NodeName(nodeName2Str);
            rscName = new ResourceName(rscNameStr);
            vlmNr = new VolumeNumber(vlmNrInt);

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
            rscDfn = ResourceDefinitionData.getInstance( // accDeniedExc3
                accCtx,
                rscName,
                null, // rscFalgs are only needed if we want to persist this object
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

                nodeNotFoundEntry.setReturnCode(RC_VLM_CONN_CRT_FAIL_NOT_FOUND_NODE);
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
                nodeNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            if (rscDfn == null)
            {
                ApiCallRcEntry rscDfnNotFoundEntry = new ApiCallRcEntry();
                rscDfnNotFoundEntry.setReturnCode(RC_VLM_CONN_CRT_FAIL_NOT_FOUND_RSC_DFN);
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
                rscDfnNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                apiCallRc.addEntry(rscDfnNotFoundEntry);
            }
            else
            {
                rsc1 = ResourceData.getInstance( // accDeniedExc4
                    accCtx,
                    rscDfn,
                    node1,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );
                rsc2 = ResourceData.getInstance( // accDeniedExc5
                    accCtx,
                    rscDfn,
                    node2,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );

                vlmDfn = VolumeDefinitionData.getInstance( // accDeniedExc6
                    accCtx,
                    rscDfn,
                    vlmNr,
                    null,  // minorNumber only needed if we want to persist this entry
                    0,     // size only needed if we want to persist this entry
                    null,  // vlmFlags only needed if we want to persist this entry
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
                    rscNotFoundEntry.setReturnCode(RC_VLM_CONN_CRT_FAIL_NOT_FOUND_RSC);
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
                    rscNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                    apiCallRc.addEntry(rscNotFoundEntry);
                }
                else
                if (vlmDfn == null)
                {
                    ApiCallRcEntry vlmDfnNotFoundEntry = new ApiCallRcEntry();
                    vlmDfnNotFoundEntry.setReturnCode(RC_VLM_CONN_CRT_FAIL_NOT_FOUND_VLM_DFN);
                    vlmDfnNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified volume definition %d in resource definition '%s' could not be found in the database",
                            vlmNrInt,
                            rscNameStr
                        )
                    );
                    vlmDfnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    vlmDfnNotFoundEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                    vlmDfnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    vlmDfnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    vlmDfnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                    vlmDfnNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                    apiCallRc.addEntry(vlmDfnNotFoundEntry);
                }
                else
                {
                    vlm1 = VolumeData.getInstance( // accDeniedExc7
                        accCtx,
                        rsc1,
                        vlmDfn,
                        null, // storPool only needed if we want to persist this entry
                        null, // blockDevicePath only needed if we want to persist this entry
                        null, // metaDiskPath only needed if we want to persist this entry
                        null, // vlmFlags only needed if we want to persist this entry
                        transMgr,
                        false, // do not persist this entry
                        false // do not throw exception if the entry exists
                    );
                    vlm2 = VolumeData.getInstance( // accDeniedExc8
                        accCtx,
                        rsc2,
                        vlmDfn,
                        null, // storPool only needed if we want to persist this entry
                        null, // blockDevicePath only needed if we want to persist this entry
                        null, // metaDiskPath only needed if we want to persist this entry
                        null, // vlmFlags only needed if we want to persist this entry
                        transMgr,
                        false, // do not persist this entry
                        false // do not throw exception if the entry exists
                    );

                    if (vlm1 == null || vlm2 == null)
                    {
                        String missingVlmNode;
                        if (vlm1 == null)
                        {
                            missingVlmNode = nodeName1Str;
                        }
                        else
                        {
                            missingVlmNode = nodeName2Str;
                        }

                        ApiCallRcEntry vlmNotFoundEntry = new ApiCallRcEntry();
                        vlmNotFoundEntry.setReturnCode(RC_VLM_CONN_CRT_FAIL_NOT_FOUND_VLM);
                        vlmNotFoundEntry.setCauseFormat(
                            String.format(
                                "The specified volume %d on resource '%s' on node '%s' could not be found in the database",
                                vlmNrInt,
                                rscNameStr,
                                missingVlmNode
                            )
                        );
                        vlmNotFoundEntry.putVariable(KEY_NODE_NAME, missingVlmNode);
                        vlmNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                        vlmNotFoundEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                        vlmNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                        vlmNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                        vlmNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                        vlmNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                        apiCallRc.addEntry(vlmNotFoundEntry);
                    }
                    else
                    {
                        VolumeConnectionData vlmConn = VolumeConnectionData.getInstance( // accDeniedExc8
                            accCtx,
                            vlm1,
                            vlm2,
                            transMgr,
                            true, // persist this entry
                            true // throw exception if the entry exists
                        );
                        vlmConn.setConnection(transMgr);
                        vlmConn.getProps(accCtx).map().putAll(vlmConnPropsMap);
                        transMgr.commit();

                        ApiCallRcEntry successEntry = new ApiCallRcEntry();
                        successEntry.setReturnCodeBit(RC_VLM_CONN_CREATED);
                        String successMessage = String.format(
                            "Volume connection between nodes '%s' and '%s' on resource '%s' on volume number %d successfully created.",
                            nodeName1Str,
                            nodeName2Str,
                            rscNameStr,
                            vlmNrInt
                        );
                        successEntry.setMessageFormat(successMessage);
                        successEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                        successEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                        successEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                        successEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                        successEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                        successEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                        successEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                        successEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                        apiCallRc.addEntry(successEntry);
                        controller.getErrorReporter().logInfo(successMessage);
                    }
                }
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to create a new volume connection between nodes " +
                    "'%s' and '%s' on resource '%s' on volume %d.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

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
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_INVLD_NODE_NAME);
                entry.putVariable(KEY_NODE_NAME, invalidNodeName);
            }
            else
            {
                errorMessage = String.format(
                    "The given resource name '%s' is invalid.",
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_INVLD_RSC_NAME);
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
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage = String.format(
                "The given volume number %d is invalid. Valid Ranges: [%d - %d]",
                vlmNrInt,
                VolumeNumber.VOLUME_NR_MIN,
                VolumeNumber.VOLUME_NR_MAX
            );
            entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_INVLD_VLM_NR);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));

            controller.getErrorReporter().reportError(
                valOutOfRangeExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(valOutOfRangeExc.getMessage());

            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

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

                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_ACC_DENIED_NODE);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
            }
            else
            if (rscDfn == null)
            { // handle accDeniedExc3
                action = String.format(
                    "access the resource definition '%s'.",
                    rscNameStr
                );

                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_ACC_DENIED_RSC_DFN);
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
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_ACC_DENIED_RSC);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }
            else
            if (vlmDfn == null)
            { // handle accDeniedExc6
                action = String.format(
                    "access the volume definition %d on resource definition '%s'.",
                    vlmNrInt,
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_ACC_DENIED_VLM_DFN);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            }
            else
            if (vlm1 == null || vlm2 == null)
            {// handle accDeniedExc4 & accDeniedExc5
                String accDeniedNodeNameStr = vlm1 == null ? nodeName1Str : nodeName2Str;
                action = String.format(
                    "access the volume %d on resource '%s' on node '%s'.",
                    vlmNrInt,
                    rscNameStr,
                    accDeniedNodeNameStr
                );
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_ACC_DENIED_VLM);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            }
            else
            { // handle accDeniedExc8
                action = String.format(
                    "create the volume connection between nodes '%s' and '%s' on resource '%s' on " +
                        "volume %d.",
                    nodeName1Str,
                    nodeName2Str,
                    rscNameStr,
                    vlmNrInt
                );
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_ACC_DENIED_VLM_CONN);
                entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
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
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException | MdException exc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            if (node1 == null || node2 == null ||
                rscDfn == null ||
                rsc1 == null || rsc2 == null ||
                vlmDfn == null || (exc instanceof MdException) ||
                vlm1 == null || vlm2 == null)
            {
                String implErrorMessage;
                if (node1 == null || node2 == null)
                {
                    String failedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s",
                        failedNodeNameStr
                    );
                }
                else
                if (rscDfn == null)
                {
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. RscName=%s",
                        rscNameStr
                    );
                }
                else
                if (rsc1 == null || rsc2 == null)
                {
                    String failedNodeNameStr = rsc1 == null ? nodeName1Str : nodeName2Str;
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s, ResName=%s",
                        failedNodeNameStr,
                        rscNameStr
                    );
                }
                else
                if (vlmDfn == null || (exc instanceof MdException))
                {
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. ResName=%s, VlmNr=%d",
                        rscNameStr,
                        vlmNrInt
                    );
                }
                else
                {
                    String failedNodeNameStr = vlm1 == null ? nodeName1Str : nodeName2Str;
                    implErrorMessage = String.format(
                        ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s, ResName=%s, vlmNr=%d",
                        failedNodeNameStr,
                        rscNameStr,
                        vlmNrInt
                    );
                }
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        implErrorMessage,
                        exc
                    )
                );
                entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_IMPL_ERROR);
                errorMessage = String.format(
                    "Failed to create the volume connection between the nodes '%s' and '%s' on resource " +
                        "'%s' on volume %d due to an implementation error.",
                    nodeName1Str,
                    nodeName2Str,
                    rscNameStr,
                    vlmNrInt
                );
            }
            else
            {
                entry.setReturnCode(RC_VLM_CONN_CRT_FAIL_EXISTS_VLM_CONN);
                errorMessage = String.format(
                    "A volume connection between the two nodes '%s' and '%s' on resource '%s' " +
                        "on volume %d already exists. ",
                    nodeName1Str,
                    nodeName2Str,
                    rscNameStr,
                    vlmNrInt
                );
            }

            entry.setCauseFormat(exc.getMessage());
            entry.setMessageFormat(errorMessage);
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while creating a volume connection between nodes '%s' "+
                    "and '%s' on resource '%s' on volume number %d.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));
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
                        "A database error occured while trying to rollback the creation of a volume connection " +
                            "between the nodes '%s' and '%s' on the resource '%s' for volume number %d.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr,
                        vlmNrInt
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                    entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                    entry.putVariable(KEY_RSC_NAME, rscNameStr);
                    entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                    entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                    entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    public ApiCallRc deleteVolumeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        String rscNameStr,
        int vlmNrInt
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;

        NodeName nodeName1 = null;
        NodeName nodeName2 = null;
        ResourceName rscName = null;
        VolumeNumber vlmNr = null;
        NodeData node1 = null;
        NodeData node2 = null;
        ResourceDefinitionData rscDfn = null;
        ResourceData rsc1 = null;
        ResourceData rsc2 = null;
        VolumeDefinitionData vlmDfn = null;
        VolumeData vlm1 = null;
        VolumeData vlm2 = null;
        VolumeConnectionData vlmConn = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName1 = new NodeName(nodeName1Str);
            nodeName2 = new NodeName(nodeName2Str);
            rscName = new ResourceName(rscNameStr);
            vlmNr = new VolumeNumber(vlmNrInt);

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
            rscDfn = ResourceDefinitionData.getInstance( // accDeniedExc3
                accCtx,
                rscName,
                null, // rscFalgs are only needed if we want to persist this object
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

                nodeNotFoundEntry.setReturnCode(RC_VLM_CONN_DEL_FAIL_NOT_FOUND_NODE);
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
                nodeNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
            if (rscDfn == null)
            {
                ApiCallRcEntry rscDfnNotFoundEntry = new ApiCallRcEntry();
                rscDfnNotFoundEntry.setReturnCode(RC_VLM_CONN_DEL_FAIL_NOT_FOUND_RSC_DFN);
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
                rscDfnNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                apiCallRc.addEntry(rscDfnNotFoundEntry);
            }
            else
            {
                rsc1 = ResourceData.getInstance( // accDeniedExc4
                    accCtx,
                    rscDfn,
                    node1,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );
                rsc2 = ResourceData.getInstance( // accDeniedExc5
                    accCtx,
                    rscDfn,
                    node2,
                    null, // nodeId only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                );

                vlmDfn = VolumeDefinitionData.getInstance( // accDeniedExc6
                    accCtx,
                    rscDfn,
                    vlmNr,
                    null,  // minorNumber only needed if we want to persist this entry
                    0,     // size only needed if we want to persist this entry
                    null,  // vlmFlags only needed if we want to persist this entry
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
                    rscNotFoundEntry.setReturnCode(RC_VLM_CONN_DEL_FAIL_NOT_FOUND_RSC);
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
                    rscNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                    apiCallRc.addEntry(rscNotFoundEntry);
                }
                else
                if (vlmDfn == null)
                {
                    ApiCallRcEntry vlmDfnNotFoundEntry = new ApiCallRcEntry();
                    vlmDfnNotFoundEntry.setReturnCode(RC_VLM_CONN_DEL_FAIL_NOT_FOUND_VLM_DFN);
                    vlmDfnNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified volume definition %d in resource definition '%s' could not be found in the database",
                            vlmNrInt,
                            rscNameStr
                        )
                    );
                    vlmDfnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    vlmDfnNotFoundEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                    vlmDfnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                    vlmDfnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                    vlmDfnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                    vlmDfnNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                    apiCallRc.addEntry(vlmDfnNotFoundEntry);
                }
                else
                {
                    vlm1 = VolumeData.getInstance( // accDeniedExc7
                        accCtx,
                        rsc1,
                        vlmDfn,
                        null, // storPool only needed if we want to persist this entry
                        null, // blockDevicePath only needed if we want to persist this entry
                        null, // metaDiskPath only needed if we want to persist this entry
                        null, // vlmFlags only needed if we want to persist this entry
                        transMgr,
                        false, // do not persist this entry
                        false // do not throw exception if the entry exists
                    );
                    vlm2 = VolumeData.getInstance( // accDeniedExc8
                        accCtx,
                        rsc2,
                        vlmDfn,
                        null, // storPool only needed if we want to persist this entry
                        null, // blockDevicePath only needed if we want to persist this entry
                        null, // metaDiskPath only needed if we want to persist this entry
                        null, // vlmFlags only needed if we want to persist this entry
                        transMgr,
                        false, // do not persist this entry
                        false // do not throw exception if the entry exists
                    );

                    if (vlm1 == null || vlm2 == null)
                    {
                        String missingVlmNode;
                        if (vlm1 == null)
                        {
                            missingVlmNode = nodeName1Str;
                        }
                        else
                        {
                            missingVlmNode = nodeName2Str;
                        }

                        ApiCallRcEntry vlmNotFoundEntry = new ApiCallRcEntry();
                        vlmNotFoundEntry.setReturnCode(RC_VLM_CONN_DEL_FAIL_NOT_FOUND_VLM);
                        vlmNotFoundEntry.setCauseFormat(
                            String.format(
                                "The specified volume %d on resource '%s' on node '%s' could not be found in the database",
                                vlmNrInt,
                                rscNameStr,
                                missingVlmNode
                            )
                        );
                        vlmNotFoundEntry.putVariable(KEY_NODE_NAME, missingVlmNode);
                        vlmNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                        vlmNotFoundEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                        vlmNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                        vlmNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                        vlmNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                        vlmNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                        apiCallRc.addEntry(vlmNotFoundEntry);
                    }
                    else
                    {
                        vlmConn = VolumeConnectionData.getInstance( // accDeniedExc8
                            accCtx,
                            vlm1,
                            vlm2,
                            transMgr,
                            false, // do not persist this entry
                            false // do not throw exception if the entry exists
                        );
                        if (vlmConn == null)
                        {
                            ApiCallRcEntry vlmConnNotFoundEntry = new ApiCallRcEntry();
                            vlmConnNotFoundEntry.setReturnCode(RC_VLM_CONN_DEL_NOT_FOUND);
                            vlmConnNotFoundEntry.setCauseFormat(
                                String.format(
                                    "The specified volume collection with volume number %d on resource '%s' "+
                                        "between node '%s' and node '%s' could not be found in the database",
                                    vlmNrInt,
                                    rscNameStr,
                                    nodeName1Str,
                                    nodeName2Str
                                )
                            );
                            vlmConnNotFoundEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                            vlmConnNotFoundEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                            vlmConnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                            vlmConnNotFoundEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                            vlmConnNotFoundEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                            vlmConnNotFoundEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                            vlmConnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                            vlmConnNotFoundEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                            apiCallRc.addEntry(vlmConnNotFoundEntry);
                        }
                        else
                        {
                            vlmConn.setConnection(transMgr);
                            vlmConn.delete(accCtx); // accDeniedExc9

                            ApiCallRcEntry successEntry = new ApiCallRcEntry();
                            successEntry.setReturnCodeBit(RC_VLM_CONN_DELETED);
                            String successMessage = String.format(
                                "Volume connection between nodes '%s' and '%s' on resource '%s' on " +
                                    "volume %d successfully deleted.",
                                nodeName1Str,
                                nodeName2Str,
                                rscNameStr,
                                vlmNrInt
                            );
                            successEntry.setMessageFormat(successMessage);
                            successEntry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                            successEntry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                            successEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                            successEntry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
                            successEntry.putObjRef(KEY_1ST_NODE, nodeName1Str);
                            successEntry.putObjRef(KEY_2ND_NODE, nodeName2Str);
                            successEntry.putObjRef(KEY_RSC_DFN, rscNameStr);
                            successEntry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

                            apiCallRc.addEntry(successEntry);
                            controller.getErrorReporter().logInfo(successMessage);
                        }
                    }
                }
            }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to delete the volume connection between nodes " +
                    "'%s' and '%s' on resource '%s' on volume %d.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
            controller.getErrorReporter().reportError(
                sqlExc,
                accCtx,
                client,
                errorMessage
            );

            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

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
                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_INVLD_NODE_NAME);
                entry.putVariable(KEY_NODE_NAME, invalidNodeName);
            }
            else
            {
                errorMessage = String.format(
                    "The given resource name '%s' is invalid.",
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_INVLD_RSC_NAME);
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
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valOutOfRangeExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage = String.format(
                "The given volume number %d is invalid. Valid Ranges: [%d - %d]",
                vlmNrInt,
                VolumeNumber.VOLUME_NR_MIN,
                VolumeNumber.VOLUME_NR_MAX
            );
            entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_INVLD_VLM_NR);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));

            controller.getErrorReporter().reportError(
                valOutOfRangeExc,
                accCtx,
                client,
                errorMessage
            );

            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(valOutOfRangeExc.getMessage());

            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

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

                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_ACC_DENIED_NODE);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
            }
            else
            if (rscDfn == null)
            { // handle accDeniedExc3
                action = String.format(
                    "access the resource definition '%s'.",
                    rscNameStr
                );

                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_ACC_DENIED_RSC_DFN);
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
                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_ACC_DENIED_RSC);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
            }
            else
            if (vlmDfn == null)
            { // handle accDeniedExc6
                action = String.format(
                    "access the volume definition %d on resource definition '%s'.",
                    vlmNrInt,
                    rscNameStr
                );
                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_ACC_DENIED_VLM_DFN);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            }
            else
            if (vlm1 == null || vlm2 == null)
            {// handle accDeniedExc4 & accDeniedExc5
                String accDeniedNodeNameStr = vlm1 == null ? nodeName1Str : nodeName2Str;
                action = String.format(
                    "access the volume %d on resource '%s' on node '%s'.",
                    vlmNrInt,
                    rscNameStr,
                    accDeniedNodeNameStr
                );
                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_ACC_DENIED_VLM);
                entry.putVariable(KEY_NODE_NAME, accDeniedNodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            }
            else
            { // handle accDeniedExc8 & accDeniedExc9
                if (vlmConn == null)
                {
                    action = String.format(
                        "access the volume connection between nodes '%s' and '%s' on resource '%s' on " +
                            "volume %d.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr,
                        vlmNrInt
                    );
                }
                else
                {
                    action = String.format(
                        "delete the volume connection between nodes '%s' and '%s' on resource '%s' on " +
                            "volume %d.",
                        nodeName1Str,
                        nodeName2Str,
                        rscNameStr,
                        vlmNrInt
                    );
                }
                entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_ACC_DENIED_VLM_CONN);
                entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
                entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
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
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException | MdException exc)
        {
            String errorMessage;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String implErrorMessage;
            if (node1 == null || node2 == null)
            {
                String failedNodeNameStr = node1 == null ? nodeName1Str : nodeName2Str;
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s",
                    failedNodeNameStr
                );
            }
            else
            if (rscDfn == null)
            {
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. RscName=%s",
                    rscNameStr
                );
            }
            else
            if (rsc1 == null || rsc2 == null)
            {
                String failedNodeNameStr = rsc1 == null ? nodeName1Str : nodeName2Str;
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s, ResName=%s",
                    failedNodeNameStr,
                    rscNameStr
                );
            }
            else
            if (vlmDfn == null || (exc instanceof MdException))
            {
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. ResName=%s, VlmNr=%d",
                    rscNameStr,
                    vlmNrInt
                );
            }
            else
            {
                String failedNodeNameStr = vlm1 == null ? nodeName1Str : nodeName2Str;
                implErrorMessage = String.format(
                    ".getInstance was called with failIfExists=false, still threw an DrbdDataAlreadyExistsException. NodeName=%s, ResName=%s, vlmNr=%d",
                    failedNodeNameStr,
                    rscNameStr,
                    vlmNrInt
                );
            }
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    implErrorMessage,
                    exc
                )
            );
            errorMessage = String.format(
                "Failed to create the volume connection between the nodes '%s' and '%s' on resource " +
                    "'%s' on volume %d due to an implementation error.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );

            entry.setReturnCodeBit(RC_VLM_CONN_CRT_FAIL_IMPL_ERROR);
            entry.setCauseFormat(exc.getMessage());
            entry.setMessageFormat(errorMessage);
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting a volume connection between nodes '%s' "+
                    "and '%s' on resource '%s' on volume number %d.",
                nodeName1Str,
                nodeName2Str,
                rscNameStr,
                vlmNrInt
            );
            controller.getErrorReporter().reportError(
                exc,
                accCtx,
                client,
                errorMessage
            );
            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_VLM_CONN_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_1ST_NODE_NAME, nodeName1Str);
            entry.putVariable(KEY_2ND_NODE_NAME, nodeName2Str);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_VLM_NR, Integer.toString(vlmNrInt));
            entry.putObjRef(KEY_1ST_NODE, nodeName1Str);
            entry.putObjRef(KEY_2ND_NODE, nodeName2Str);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_VLM_NR, Integer.toString(vlmNrInt));
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
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

}
