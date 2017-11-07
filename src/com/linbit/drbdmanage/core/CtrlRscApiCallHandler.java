package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.DrbdDataAlreadyExistsException;
import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeData;
import com.linbit.drbdmanage.NodeId;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.PriorityProps;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.Resource.RscFlags;
import com.linbit.drbdmanage.ResourceData;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionData;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPool;
import com.linbit.drbdmanage.StorPoolData;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolDefinitionData;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.Volume.VlmApi;
import com.linbit.drbdmanage.Volume.VlmFlags;
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeNumber;
import com.linbit.drbdmanage.api.ApiCallRc;
import com.linbit.drbdmanage.api.ApiCallRcImpl;
import com.linbit.drbdmanage.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.api.protobuf.controller.interfaces.ResourceDataSerializer;
import com.linbit.drbdmanage.netcom.IllegalMessageStateException;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.propscon.InvalidKeyException;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

class CtrlRscApiCallHandler
{
    private final Controller controller;
    private final ResourceDataSerializer serializer;
    private final AccessContext apiCtx;

    CtrlRscApiCallHandler(
        Controller controllerRef,
        ResourceDataSerializer serializerRef,
        AccessContext apiCtxRef
    )
    {
        controller = controllerRef;
        serializer = serializerRef;
        apiCtx = apiCtxRef;
    }

    public ApiCallRc createResource(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr,
        Map<String, String> rscPropsMap,
        List<VlmApi> vlmApiList
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;

        NodeName nodeName = null;
        ResourceName rscName = null;

        NodeData node = null;
        ResourceDefinitionData rscDfn = null;

        NodeId nodeId = null;

        ResourceData rsc = null;
        VlmApi currentVlmApi = null;
        VolumeNumber volNr = null;
        VolumeDefinition vlmDfn = null;
        String storPoolNameStr = null;
        Props vlmProps = null;
        Props rscProps = null;
        Props nodeProps = null;
        StorPoolName storPoolName = null;
        StorPoolDefinition storPoolDfn = null;
        StorPool storPool = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName = new NodeName(nodeNameStr); // invalidNameExc1
            rscName = new ResourceName(rscNameStr); // invalidNameExc2

            node = NodeData.getInstance(
                // accDeniedExc1, dataAlreadyExistsExc0
                accCtx,
                nodeName,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );
            rscDfn = ResourceDefinitionData.getInstance(
                // accDeniedExc2, dataAlreadyExistsExc0
                accCtx,
                rscName,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
            );

            if (node == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                nodeNotFoundEntry.setReturnCode(RC_RSC_CRT_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified node '%s' could not be found in the database",
                        nodeNameStr
                    )
                );
                nodeNotFoundEntry.setCorrectionFormat(
                    String.format(
                        "Create a node with the name '%s' first.",
                        nodeNameStr
                    )
                );
                nodeNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
                if (rscDfn == null)
                {
                    ApiCallRcEntry rscDfnNotFoundEntry = new ApiCallRcEntry();
                    rscDfnNotFoundEntry.setReturnCode(RC_RSC_CRT_FAIL_NOT_FOUND_RSC_DFN);
                    rscDfnNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified resource definition '%s' could not be found in the database",
                            rscNameStr
                        )
                    );
                    rscDfnNotFoundEntry.setCorrectionFormat(
                        String.format(
                            "Create a resource definition with the name '%s' first.",
                            rscNameStr
                        )
                    );
                    rscDfnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    rscDfnNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                    rscDfnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(rscDfnNotFoundEntry);
                }
                else
                {
                    nodeId = getNextNodeId(rscDfn.iterateResource(accCtx)); // accDenied should have happened on
                                                                            // accDenied2
                    RscFlags[] initFlags = null;

                    ApiCallRcImpl successApiCallRc = new ApiCallRcImpl();
                    boolean success = true;

                    rsc = ResourceData.getInstance(
                        // accDeniedExc3, dataAlreadyExistsExc1
                        accCtx,
                        rscDfn,
                        node,
                        nodeId,
                        initFlags,
                        transMgr,
                        true, // persist this entry
                        true // throw exception if the entry exists
                    );
                    rsc.setConnection(transMgr); // maybe volumes will be created
                    rsc.getProps(accCtx).map().putAll(rscPropsMap); // accDeniedExc4

                    ApiCallRcEntry rscSuccess = new ApiCallRcEntry();
                    String rscSuccessMsg = String.format(
                        "Resource '%s' successfully created on node '%s'.",
                        rscNameStr,
                        nodeNameStr
                    );

                    rscSuccess.setMessageFormat(rscSuccessMsg);
                    rscSuccess.setReturnCode(RC_RSC_CREATED);
                    rscSuccess.putObjRef(KEY_NODE, nodeNameStr);
                    rscSuccess.putObjRef(KEY_RSC_DFN, rscNameStr);
                    rscSuccess.putVariable(KEY_NODE_NAME, nodeNameStr);
                    rscSuccess.putVariable(KEY_RSC_NAME, rscNameStr);

                    successApiCallRc.addEntry(rscSuccess);

                    for (VlmApi vlmApi : vlmApiList)
                    {
                        currentVlmApi = vlmApi;

                        volNr = null;
                        vlmDfn = null;
                        storPoolName = null;
                        storPoolDfn = null;
                        storPool = null;
                        vlmProps = null;
                        rscProps = null;
                        nodeProps = null;

                        volNr = new VolumeNumber(vlmApi.getVlmNr()); // valueOutOfRangeExc1
                        vlmDfn = rscDfn.getVolumeDfn(accCtx, volNr); // accDeniedExc5

                        if (vlmDfn == null)
                        {
                            success = false;
                            ApiCallRcEntry entry = new ApiCallRcEntry();
                            String errorMessage = String.format(
                                "Volume definition with volume number %d could not be found.",
                                volNr.value
                            );
                            entry.setReturnCode(RC_RSC_CRT_FAIL_NOT_FOUND_VLM_DFN);
                            controller.getErrorReporter().reportError(
                                new NullPointerException("Dependency not found"),
                                accCtx,
                                client,
                                errorMessage
                            );
                            entry.setMessageFormat(errorMessage);
                            entry.putObjRef(KEY_NODE, nodeNameStr);
                            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                            entry.putObjRef(KEY_VLM_NR, Integer.toString(volNr.value));
                            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                            entry.putVariable(KEY_RSC_NAME, rscNameStr);
                            entry.putVariable(KEY_VLM_NR, Integer.toString(volNr.value));
                            break;
                        }

                        storPoolNameStr = vlmApi.getStorPoolName();
                        if (storPoolNameStr == null || "".equals(storPoolNameStr))
                        {
                            vlmProps = vlmDfn.getProps(accCtx); // accDeniedExc6
                            rscProps = rsc.getProps(accCtx); // accDeniedExc7
                            nodeProps = node.getProps(accCtx); // accDeniedExc8
                            PriorityProps prioProps = new PriorityProps(
                                vlmProps,
                                rscProps,
                                nodeProps
                            );
                            storPoolNameStr = prioProps.getProp(KEY_STOR_POOL_NAME);
                        }
                        if (storPoolNameStr == null || "".equals(storPoolNameStr))
                        {
                            storPoolNameStr = controller.getDefaultStorPoolName();
                        }

                        storPoolName = new StorPoolName(storPoolNameStr); // invalidNameExc3

                        storPoolDfn = StorPoolDefinitionData.getInstance(
                            // accDeniedExc9, dataAlreadyExistsExc0
                            accCtx,
                            storPoolName,
                            transMgr,
                            false, // do not persist this entry
                            false // do not throw exception if the entry exists
                        );
                        if (storPoolDfn != null)
                        {
                            storPool = StorPoolData.getInstance(
                                // accDeniedExc10, dataAlreadyExistsExc0
                                accCtx,
                                node,
                                storPoolDfn,
                                null, // controller must not have a storage driver defined
                                transMgr,
                                false, // do not create an instance of the (not defined) storage driver
                                false, // do not persist this entry
                                false // do not throw exception if the entry exists
                            );
                        }
                        if (storPoolDfn == null || storPool == null)
                        {
                            success = false;

                            ApiCallRcEntry entry = new ApiCallRcEntry();
                            String errorMessage;
                            if (storPoolDfn == null)
                            {
                                errorMessage = String.format(
                                    "Storage pool definition '%s' could not be found.",
                                    storPoolNameStr
                                );
                                entry.setReturnCode(RC_RSC_CRT_FAIL_NOT_FOUND_STOR_POOL_DFN);
                            }
                            else
                            {
                                errorMessage = String.format(
                                    "Storage pool '%s' on node '%s' could not be found.",
                                    storPoolNameStr,
                                    nodeNameStr
                                );
                                entry.setReturnCode(RC_RSC_CRT_FAIL_NOT_FOUND_STOR_POOL);
                            }
                            controller.getErrorReporter().reportError(
                                new NullPointerException("Dependency not found"),
                                accCtx,
                                client,
                                errorMessage
                            );
                            entry.setMessageFormat(errorMessage);
                            entry.putObjRef(KEY_NODE, nodeNameStr);
                            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                            entry.putObjRef(KEY_VLM_NR, Integer.toString(volNr.value));
                            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                            entry.putVariable(KEY_RSC_NAME, rscNameStr);
                            entry.putVariable(KEY_VLM_NR, Integer.toString(volNr.value));
                            break;
                        }
                        else
                        {
                            VlmFlags[] vlmFlags = null;

                            VolumeData vlmData = VolumeData.getInstance(
                                // accDeniedExc11, dataAlreadyExistsExc2
                                accCtx,
                                rsc,
                                vlmDfn,
                                storPool,
                                vlmApi.getBlockDevice(),
                                vlmApi.getMetaDisk(),
                                vlmFlags,
                                transMgr,
                                true, // persist this entry
                                true // throw exception if the entry exists
                            );
                            vlmData.setConnection(transMgr);
                            vlmData.getProps(accCtx).map().putAll(vlmApi.getVlmProps());

                            ApiCallRcEntry vlmSuccess = new ApiCallRcEntry();
                            vlmSuccess.setMessageFormat(
                                String.format(
                                    "Volume with number %d created successfully on node '%s' on resource '%s'.",
                                    vlmApi.getVlmNr(),
                                    nodeNameStr,
                                    rscNameStr
                                )
                            );
                            vlmSuccess.setReturnCode(RC_VLM_CREATED);
                            vlmSuccess.putVariable(KEY_NODE_NAME, nodeNameStr);
                            vlmSuccess.putVariable(KEY_RSC_NAME, rscNameStr);
                            vlmSuccess.putVariable(KEY_VLM_NR, Integer.toString(vlmApi.getVlmNr()));
                            vlmSuccess.putObjRef(KEY_NODE, nodeNameStr);
                            vlmSuccess.putObjRef(KEY_RSC_DFN, rscNameStr);
                            vlmSuccess.putObjRef(KEY_VLM_NR, Integer.toString(vlmApi.getVlmNr()));

                            successApiCallRc.addEntry(vlmSuccess);
                        }
                    }

                    if (success)
                    {
                        transMgr.commit();

                        // if everything worked fine, just replace the returned rcApiCall with the
                        // already filled successApiCallRc. otherwise, this line does not get executed anyways
                        apiCallRc = successApiCallRc;
                        controller.getErrorReporter().logInfo(rscSuccessMsg);

                        sendToNode(accCtx, client, rsc);

                        // TODO: tell satellite(s) to do their job
                        // TODO: if a satellite confirms creation, also log it to controller.info
                    }
                }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to create the resource '%s' on node '%s'.",
                nodeNameStr,
                rscNameStr
                );
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CRT_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (nodeName == null)
            { // invalidNameExc1
                errorMessage = String.format("Given node name '%s' is invalid.", nodeNameStr);
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_INVLD_NODE_NAME);
            }
            else
                if (rscName == null)
                { // invalidNameExc2
                    errorMessage = String.format("Given resource name '%s' is invalid.", rscNameStr);
                    entry.putVariable(KEY_RSC_NAME, rscNameStr);
                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_INVLD_RSC_NAME);
                }
                else
                { // invalidNameExc3
                    errorMessage = String.format("Given storage pool name '%s' is invalid.", storPoolNameStr);
                    entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_INVLD_STOR_POOL_NAME);

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
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action = "Given user has no permission to ";
            if (node == null)
            { // accDeniedExc1
                action += String.format(
                    "access the node '%s'.",
                    nodeNameStr
                    );
                    entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_NODE);
            }
            else
                if (rscDfn == null)
                { // accDeniedExc2
                    action += String.format(
                        "access the resource definition '%s'.",
                        rscNameStr
                        );
                        entry.putVariable(KEY_RSC_NAME, rscNameStr);
                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_RSC_DFN);
                }
                else
                    if (rsc == null || currentVlmApi == null)
                    { // accDeniedExc3 & accDeniedExc4
                        action += String.format(
                            "access the resource '%s' on node '%s'.",
                            rscNameStr,
                            nodeNameStr
                            );
                            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                        entry.putVariable(KEY_RSC_NAME, rscNameStr);
                        entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_RSC);
                    }
                    else
                        if (vlmDfn == null)
                        { // accDeniedExc5
                            action += String.format(
                                "access the volume definition with volume number %d on resource '%s' on node '%s'.",
                                currentVlmApi.getVlmNr(),
                                rscNameStr,
                                nodeNameStr
                                );
                                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                            entry.putVariable(KEY_RSC_NAME, rscNameStr);
                            entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                            entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                            entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_VLM_DFN);
                        }
                        else
                            // accDeniedExc6, 7 or 8 cannot happen as those should have triggered
                            // accDeniedExc5, 4 or 1 respectively
                            if (storPoolDfn == null)
                            { // accDeniedExc9
                                action += String.format(
                                    "access the storage pool definition '%s'.",
                                    storPoolNameStr
                                    );
                                    entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                                entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);
                                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_STOR_POOL_DFN);
                            }
                            else
                                if (storPool == null)
                                { // accDeniedExc10
                                    action += String.format(
                                        "access the storage pool '%s' on node '%s'.",
                                        storPoolNameStr,
                                        nodeNameStr
                                        );
                                        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                                    entry.putVariable(KEY_STOR_POOL_NAME, storPoolNameStr);
                                    entry.putObjRef(KEY_STOR_POOL_DFN, storPoolNameStr);
                                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_STOR_POOL);
                                }
                                else
                                { // accDeniedExc11
                                    action += String.format(
                                        "create a new volume with volume number %d on resource '%s' on node '%s'.",
                                        currentVlmApi.getVlmNr(),
                                        rscNameStr,
                                        nodeNameStr
                                        );
                                        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                                    entry.putVariable(KEY_RSC_NAME, rscNameStr);
                                    entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                                    entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_ACC_DENIED_VLM);
                                }
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                action
                );
                entry.setCauseFormat(accDeniedExc.getMessage());
            entry.setMessageFormat(action);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        {
            String errorMsgFormat;
            ApiCallRcEntry entry = new ApiCallRcEntry();
            // dataAlreadyExistsExc0 cannot happen
            if (rsc == null)
            { // dataAlreadyExistsExc1
                errorMsgFormat = String.format(
                    "Resource '%s' could not be created as it already exists on node '%s'.",
                    rscNameStr,
                    nodeNameStr
                    );
                    entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_EXISTS_RSC);
            }
            else
            { // dataAlreadyExistsExc2
                errorMsgFormat = String.format(
                    "Volume with volume number %d could not be created as it already exists on " +
                        "resource '%s' on node '%s'.",
                    currentVlmApi.getVlmNr(),
                    rscNameStr,
                    nodeNameStr
                        );
                        entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.putObjRef(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
                entry.setReturnCodeBit(RC_RSC_CRT_FAIL_EXISTS_NODE);
            }

            controller.getErrorReporter().reportError(
                dataAlreadyExistsExc,
                accCtx,
                client,
                errorMsgFormat
                );
                entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.setMessageFormat(errorMsgFormat);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            String errorMsgFormat;
            ApiCallRcEntry entry = new ApiCallRcEntry();

            // valueOutOfRangeExc1
            errorMsgFormat = String.format(
                "Volume number %d is out of its valid range (%d - %d)",
                currentVlmApi.getVlmNr(),
                VolumeNumber.VOLUME_NR_MIN,
                VolumeNumber.VOLUME_NR_MAX
            );
            entry.putVariable(KEY_VLM_NR, Integer.toString(currentVlmApi.getVlmNr()));
            entry.setReturnCode(RC_RSC_CRT_FAIL_INVLD_VLM_NR);
            controller.getErrorReporter().reportError(
                valueOutOfRangeExc,
                accCtx,
                client,
                errorMsgFormat
                );
                entry.setCauseFormat(valueOutOfRangeExc.getMessage());
            entry.setMessageFormat(errorMsgFormat);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvalidKeyException implExc)
        {
            String errorMessage;
            if (implExc instanceof InvalidKeyException)
            {
                errorMessage = String.format(
                    "The property key '%s' has thrown an InvalidKeyException " +
                        "(Node name: '%s', resource name: '%s')",
                    KEY_STOR_POOL_NAME,
                    nodeNameStr,
                    rscNameStr
                        );
                        }
                        else
            {
                errorMessage = String.format(
                    "A dynamic class instantiation related exception occured while this method shoud " +
                        "not create such instances (Node name: '%s', resource name: '%s', volNr: %d, stor pool name: '%s')",
                    nodeNameStr,
                    rscNameStr,
                    volNr.value,
                    storPoolNameStr
                        );
                        }
                        controller.getErrorReporter().reportError(
                            new ImplementationError(
                                errorMessage,
                                implExc
                                )
                            );

                            ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CRT_FAIL_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to create the resource '%s' on node '%s' due to an implementation error.",
                    rscNameStr,
                    nodeNameStr
                    )
                );
                entry.setCauseFormat(implExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while creating resource '%s' on node '%s'.",
                rscNameStr,
                nodeNameStr
                );
                controller.getErrorReporter().reportError(
                    exc,
                    accCtx,
                    client,
                    errorMessage
                    );
                    ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_CRT_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
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
                        "A database error occured while trying to rollback the creation of resource " +
                            "'%s' on node '%s'.",
                        rscNameStr,
                        nodeNameStr
                            );
                            controller.getErrorReporter().reportError(
                                sqlExc,
                                accCtx,
                                client,
                                errorMessage
                                );

                                ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_NODE, nodeNameStr);
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    private NodeId getNextNodeId(Iterator<Resource> rscIterator)
    {
        NodeId nodeId = null;
        Node[] idsInUse = new Node[NodeId.NODE_ID_MAX + 1];
        int id = -1;
        while (rscIterator.hasNext())
        {
            Resource rsc = rscIterator.next();
            int val = rsc.getNodeId().value;
            if (idsInUse[val] != null)
            {
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        String.format(
                            "NodeId '%d' is used for resource '%s' on node '%s' AND '%s'",
                            val,
                            rsc.getDefinition().getName().value,
                            idsInUse[val].getName().value,
                            rsc.getAssignedNode().getName().value
                            ),
                        null
                    )
                );
            }
            idsInUse[val] = rsc.getAssignedNode();
        }

        for (int idx = 0; idx < idsInUse.length; idx++)
        {
            if (idsInUse[idx] == null)
            {
                id = idx;
                break;
            }
        }
        try
        {
            if (id == -1)
            {
                controller.getErrorReporter().reportError(
                    new DrbdManageException(
                        String.format(
                            "Could not find valid nodeId. Most likely because the maximum count (%d)" +
                                " is already reached",
                            NodeId.NODE_ID_MAX + 1
                        )
                    )
                );
            }
            nodeId = new NodeId(id + 1);
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError("Found nodeId was invalid", valueOutOfRangeExc)
            );
        }
        return nodeId;
    }

    private void sendToNode(AccessContext accCtx, Peer client, ResourceData rsc)
    {
        try
        {
            // notify all peers that (at least one of) their resource has changed
            Iterator<Resource> rscIterator = rsc.getDefinition().iterateResource(accCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                Peer peer = currentRsc.getAssignedNode().getPeer(apiCtx);

                Message message = peer.createMessage();
                byte[] data = serializer.getChangedMessage(rsc);
                message.setData(data);
                peer.sendMessage(message);
            }

            Peer peer = rsc.getAssignedNode().getPeer(accCtx);
            Message message = peer.createMessage();
            byte[] data = serializer.serialize(rsc);
            if (data != null)
            {
                message.setData(data);
                peer.sendMessage(message);
            } // otherwise the serializer should have reported an error already
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Default tcp connector peer has not a valid ServiceName",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller could not send send a message to target node",
                    illegalMessageStateExc
                )
            );
        }
    }

    public ApiCallRc deleteResource(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr
        )
        {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        TransactionMgr transMgr = null;

        NodeName nodeName = null;
        ResourceName rscName = null;

        NodeData node = null;
        ResourceDefinitionData rscDfn = null;
        ResourceData rscData = null;

        try
        {
            transMgr = new TransactionMgr(controller.dbConnPool);

            nodeName = new NodeName(nodeNameStr);
            rscName = new ResourceName(rscNameStr);

            node = NodeData.getInstance(
                accCtx,
                nodeName,
                null, // nodeType only needed if we want to persist this entry
                null, // nodeFlags only needed if we want to persist this entry
                transMgr,
                false, // do not persist this entry
                false // do not throw exception if the entry exists
                );
                rscDfn = ResourceDefinitionData.getInstance(
                    accCtx,
                    rscName,
                    null, // port only needed if we want to persist this entry
                    null, // rscFlags only needed if we want to persist this entry
                    transMgr,
                    false, // do not persist this entry
                    false // do not throw exception if the entry exists
                    );
                    rscData = ResourceData.getInstance(
                        accCtx,
                        rscDfn,
                        node,
                        null, // nodeId only needed if we want to persist this entry
                        null, // rscFlags only needed if we want to persist this entry
                        transMgr,
                        false, // do not persist this entry
                        false // do not throw exception if the entry exists
                        );

                        if (node == null)
            {
                ApiCallRcEntry nodeNotFoundEntry = new ApiCallRcEntry();
                nodeNotFoundEntry.setReturnCode(RC_RSC_DEL_FAIL_NOT_FOUND_NODE);
                nodeNotFoundEntry.setCauseFormat(
                    String.format(
                        "The specified node '%s' could not be found in the database.",
                        nodeNameStr
                        )
                    );
                    nodeNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                nodeNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                apiCallRc.addEntry(nodeNotFoundEntry);
            }
            else
                if (rscDfn == null)
                {
                    ApiCallRcEntry rscDfnNotFoundEntry = new ApiCallRcEntry();
                    rscDfnNotFoundEntry.setReturnCode(RC_RSC_DEL_FAIL_NOT_FOUND_RSC_DFN);
                    rscDfnNotFoundEntry.setCauseFormat(
                        String.format(
                            "The specified resource definition '%s' could not be found in the database.",
                            rscNameStr
                            )
                        );
                        rscDfnNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                    rscDfnNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                    rscDfnNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                    apiCallRc.addEntry(rscDfnNotFoundEntry);
                }
                else
                    if (rscData == null)
                    {
                        ApiCallRcEntry rscNotFoundEntry = new ApiCallRcEntry();
                        rscNotFoundEntry.setReturnCode(RC_RSC_DEL_NOT_FOUND);
                        rscNotFoundEntry.setCauseFormat(
                            String.format(
                                "The specified resource '%s' on node '%s' could not be found in the database.",
                                rscNameStr,
                                nodeNameStr
                                )
                            );
                            rscNotFoundEntry.putVariable(KEY_RSC_NAME, rscNameStr);
                        rscNotFoundEntry.putVariable(KEY_NODE_NAME, nodeNameStr);
                        rscNotFoundEntry.putObjRef(KEY_NODE, nodeNameStr);
                        rscNotFoundEntry.putObjRef(KEY_RSC_DFN, rscNameStr);

                        apiCallRc.addEntry(rscNotFoundEntry);
                    }
                    else
                    {
                        rscData.setConnection(transMgr);
                        rscData.markDeleted(accCtx);
                        transMgr.commit();

                        ApiCallRcEntry entry = new ApiCallRcEntry();
                        entry.setReturnCodeBit(RC_RSC_DELETED);
                        String successMessage = String.format(
                            "Resource '%s' marked to be deleted from node '%s'.",
                            rscNameStr,
                            nodeNameStr
                            );
                            entry.setMessageFormat(successMessage);
                        entry.putObjRef(KEY_NODE, nodeNameStr);
                        entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                        entry.putObjRef(KEY_NODE_NAME, nodeNameStr);
                        entry.putVariable(KEY_RSC_NAME, rscNameStr);
                        apiCallRc.addEntry(entry);

                        // TODO: tell satellites to remove all the corresponding resources
                        // TODO: if satellites are finished (or no satellite had such a resource deployed)
                        // remove the rscDfn from the DB
                        controller.getErrorReporter().logInfo(successMessage);
                    }
        }
        catch (SQLException sqlExc)
        {
            String errorMessage = String.format(
                "A database error occured while trying to delete the resource '%s' on node '%s'.",
                nodeNameStr,
                rscNameStr
                );
                controller.getErrorReporter().reportError(
                    sqlExc,
                    accCtx,
                    client,
                    errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DEL_FAIL_SQL);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(sqlExc.getMessage());
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (InvalidNameException invalidNameExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String errorMessage;
            if (nodeName == null)
            {
                errorMessage = String.format("Given node name '%s' is invalid.", nodeNameStr);
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.setReturnCodeBit(RC_RSC_DEL_FAIL_INVLD_NODE_NAME);
            }
            else
            {
                errorMessage = String.format("Given resource name '%s' is invalid.", rscNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.setReturnCodeBit(RC_RSC_DEL_FAIL_INVLD_RSC_NAME);
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
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            ApiCallRcEntry entry = new ApiCallRcEntry();
            String action = "Given user has no permission to ";
            if (node == null)
            { // accDeniedExc1
                action += String.format(
                    "access the node '%s'.",
                    nodeNameStr
                );
                entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                entry.setReturnCodeBit(RC_RSC_DEL_FAIL_ACC_DENIED_NODE);
            }
            else
                if (rscDfn == null)
                { // accDeniedExc2
                    action += String.format(
                        "access the resource definition '%s'.",
                        rscNameStr
                    );
                    entry.putVariable(KEY_RSC_NAME, rscNameStr);
                    entry.setReturnCodeBit(RC_RSC_DEL_FAIL_ACC_DENIED_RSC_DFN);
                }
                else
                    if (rscData == null)
                    { // accDeniedExc3
                        action += String.format(
                            "access the resource '%s' on node '%s'.",
                            rscNameStr,
                            nodeNameStr
                            );
                            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                        entry.putVariable(KEY_RSC_NAME, rscNameStr);
                        entry.setReturnCodeBit(RC_RSC_DEL_FAIL_ACC_DENIED_RSC);
                    }
                    else
                    { // accDeniedExc4
                        action += String.format(
                            "delete the resource '%s' on node '%s'.",
                            rscNameStr,
                            nodeNameStr
                            );
                            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                        entry.putVariable(KEY_RSC_NAME, rscNameStr);
                        entry.setReturnCodeBit(RC_RSC_DEL_FAIL_ACC_DENIED_VLM_DFN);
                    }
            controller.getErrorReporter().reportError(
                accDeniedExc,
                accCtx,
                client,
                action
                );
                entry.setCauseFormat(accDeniedExc.getMessage());
            entry.setMessageFormat(action);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (DrbdDataAlreadyExistsException dataAlreadyExistsExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    String.format(
                        ".getInstance was called with failIfExists=false, still threw an AlreadyExistsException " +
                            "(Node name: '%s', resource name: '%s')",
                        nodeNameStr,
                        rscNameStr
                        ),
                    dataAlreadyExistsExc
                        )
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DEL_FAIL_IMPL_ERROR);
            entry.setMessageFormat(
                String.format(
                    "Failed to delete the resource '%s' on node '%s' due to an implementation error.",
                    rscNameStr,
                    nodeNameStr
                    )
                    );
                    entry.setCauseFormat(dataAlreadyExistsExc.getMessage());
            entry.putObjRef(KEY_RSC_DFN, rscNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);

            apiCallRc.addEntry(entry);
        }
        catch (Exception | ImplementationError exc)
        {
            // handle any other exception
            String errorMessage = String.format(
                "An unknown exception occured while deleting resource '%s' on node '%s'.",
                rscNameStr,
                nodeNameStr
                );
                controller.getErrorReporter().reportError(
                    exc,
                    accCtx,
                    client,
                    errorMessage
                    );
                    ApiCallRcEntry entry = new ApiCallRcEntry();
            entry.setReturnCodeBit(RC_RSC_DEL_FAIL_UNKNOWN_ERROR);
            entry.setMessageFormat(errorMessage);
            entry.setCauseFormat(exc.getMessage());
            entry.putVariable(KEY_NODE_NAME, nodeNameStr);
            entry.putVariable(KEY_RSC_NAME, rscNameStr);
            entry.putObjRef(KEY_NODE, nodeNameStr);
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
                        "A database error occured while trying to rollback the deletion of " +
                            "resource '%s' on node '%s'.",
                        rscNameStr,
                        nodeNameStr
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_RSC_DEL_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                    entry.putObjRef(KEY_NODE, nodeNameStr);
                    entry.putVariable(KEY_NODE_NAME, nodeNameStr);
                    entry.putVariable(KEY_RSC_NAME, rscNameStr);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }

        return apiCallRc;
    }

    public void respondResource(String rscNameStr, UUID rscUuid, int msgId)
    {
        try
        {
            ResourceName rscName = new ResourceName(rscNameStr);
            ResourceDefinition rscDfn = controller.rscDfnMap.get(rscName);

            Iterator<Resource> rscIterator = rscDfn.iterateResource(apiCtx);
            List<Resource> otherResources = new ArrayList<>();
            Resource localResource = null;
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();
                if (rsc.getUuid().equals(rscUuid))
                {
                    localResource = rsc;
                }
                else
                {
                    otherResources.add(rsc);
                }
            }

            if (localResource != null)
            {
                byte[] data = serializer.getRscReqResponse(msgId, localResource, otherResources);

                Peer peer = localResource.getAssignedNode().getPeer(apiCtx);

                Message response = peer.createMessage();
                response.setData(data);
                peer.sendMessage(response);
            }
            else
            {
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        String.format(
                            "A requested resource name '%s' with the uuid '%s' was not found "+
                                "in the controllers list of resources",
                                rscName,
                                rscUuid.toString()
                            ),
                        null
                    )
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite requested data for invalid resource name '" + rscNameStr + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested resource data.",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to respond to resource data request",
                    illegalMessageStateExc
                )
            );
        }
    }
}
