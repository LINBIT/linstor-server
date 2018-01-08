package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.*;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.interfaces.serializer.CtrlListSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import java.io.IOException;
import java.util.ArrayList;

class CtrlRscApiCallHandler extends AbsApiCallHandler
{
    private final CtrlSerializer<Resource> rscSerializer;
    private final CtrlListSerializer<Resource.RscApi> rscListSerializer;

    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentRscName = new ThreadLocal<>();

    CtrlRscApiCallHandler(
        Controller controllerRef,
        CtrlSerializer<Resource> rscSerializerRef,
        CtrlListSerializer<Resource.RscApi> rscListSerializerRef,
        AccessContext apiCtxRef
    )
    {
        super (
            controllerRef,
            apiCtxRef,
            ApiConsts.MASK_RSC
        );
        super.setNullOnAutoClose(
            currentNodeName,
            currentRscName
        );
        rscSerializer = rscSerializerRef;
        rscListSerializer = rscListSerializerRef;
    }

    @Override
    protected CtrlSerializer<Resource> getResourceSerializer()
    {
        return rscSerializer;
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


        try (
            AbsApiCallHandler basicallyThis = setCurrent(
                accCtx,
                client,
                ApiCallType.CREATE,
                apiCallRc,
                null,
                nodeNameStr,
                rscNameStr
            );
        )
        {
            NodeData node = loadNode(nodeNameStr, true);
            ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);

            NodeId nodeId = getNextFreeNodeId(rscDfn);

            ResourceData rsc = createResource(rscDfn, node, nodeId);
            Props rscProps = getProps(rsc);
            rscProps.map().putAll(rscPropsMap);

            Props rscDfnProps = getProps(rscDfn);
            Props nodeProps = getProps(node);

            Map<Integer, Volume> vlmMap = new TreeMap<>();
            for (VlmApi vlmApi : vlmApiList)
            {
                VolumeDefinitionData vlmDfn = loadVlmDfn(rscDfn, vlmApi.getVlmNr(), true);

                String storPoolNameStr = vlmApi.getStorPoolName();
                if (storPoolNameStr == null || "".equals(storPoolNameStr))
                {
                    PriorityProps prioProps = new PriorityProps(
                        rscProps,
                        getProps(vlmDfn),
                        rscDfnProps,
                        nodeProps
                    );
                    storPoolNameStr = prioProps.getProp(KEY_STOR_POOL_NAME);
                }
                if (storPoolNameStr == null || "".equals(storPoolNameStr))
                {
                    storPoolNameStr = controller.getDefaultStorPoolName();
                }

                StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr, true);
                StorPoolData storPool = loadStorPool(storPoolDfn, node, true);

                VolumeData vlmData = createVolume(rsc, vlmDfn, storPool, vlmApi);

                Props vlmProps = getProps(vlmData);
                vlmProps.map().putAll(vlmApi.getVlmProps());

                vlmMap.put(vlmDfn.getVolumeNumber().value, vlmData);
            }

            Iterator<VolumeDefinition> iterateVolumeDfn = getVlmDfnIterator(rscDfn);
            while (iterateVolumeDfn.hasNext())
            {
                VolumeDefinition vlmDfn = iterateVolumeDfn.next();

                currentObjRefs.get().put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
                currentVariables.get().put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

                // first check if we probably just deployed a vlm for this vlmDfn
                if (rsc.getVolume(vlmDfn.getVolumeNumber()) == null)
                {
                    // not deployed yet.

                    PriorityProps prioProps = new PriorityProps(
                        getProps(vlmDfn),
                        rscProps,
                        nodeProps
                    );
                    String storPoolNameStr = prioProps.getProp(KEY_STOR_POOL_NAME);
                    if (storPoolNameStr == null || "".equals(storPoolNameStr))
                    {
                        storPoolNameStr = controller.getDefaultStorPoolName();
                    }

                    StorPool dfltStorPool = rsc.getAssignedNode().getStorPool(
                        apiCtx,
                        asStorPoolName(storPoolNameStr)
                    );

                    if (dfltStorPool == null)
                    {
                        throw asExc(
                            new LinStorException("Dependency not found"),
                            "The default storage pool '" + storPoolNameStr + "' " +
                            "for resource '" + rsc.getDefinition().getName().displayValue + "' " +
                            "for volume number " +  vlmDfn.getVolumeNumber().value + "' " +
                            "is not deployed on node '" + rsc.getAssignedNode().getName().displayValue + "'.",
                            null, // cause
                            "The resource which should be deployed had at least one volume definition " +
                            "(volume number '" + vlmDfn.getVolumeNumber().value + "') which LinStor " +
                            "tried to automatically create. " +
                            "The default storage pool's name for this new volume was looked for in " +
                            "its volume definition's properties, its resource's properties, its node's " +
                            "properties and finally in a system wide default storage pool name defined by " +
                            "the LinStor controller.",
                            null, // correction
                            FAIL_NOT_FOUND_DFLT_STOR_POOL
                        );
                    }
                    else
                    {
                        // create missing vlm with default values
                        VolumeData vlm = createVolume(rsc, vlmDfn, dfltStorPool, null);
                        vlmMap.put(vlmDfn.getVolumeNumber().value, vlm);
                    }
                }
            }

            commit();

            // TODO: AFTER REWORK report ALL stuff
            reportSuccess(
                "Resource '" + rscNameStr + "' successfully created on node '" + nodeNameStr + "'."
            );
            for (Entry<Integer, Volume> entry : vlmMap.entrySet())
            {
                 reportSuccess(
                     "Volume with number '" + entry.getKey() + "' on resource '" +
                     entry.getValue().getResourceDefinition().getName().displayValue + "' on node '" +
                     entry.getValue().getResource().getAssignedNode().getName().displayValue + "' successfully created",
                     "Volume UUID is: " + entry.getValue().getUuid().toString()
                 );
            }
            updateSatellites(rsc);
            // TODO: if a satellite confirms creation, also log it to controller.info
        }
        catch (ApiCallHandlerFailedException ignore)
        {
            // exception already reported and added to client's answer-set
        }
        catch (Exception exc)
        {
            reportStatic(
                exc,
                "An unknown exception occured during creation of resource '" + rscNameStr + "' on node '" +
                nodeNameStr + "'.",
                null, // cause
                null, // details
                null, // corerction
                ApiConsts.RC_RSC_CRT_FAIL_UNKNOWN_ERROR,
                getObjRefs(nodeNameStr, rscNameStr),
                getVariables(nodeNameStr, rscNameStr),
                apiCallRc,
                controller,
                accCtx,
                client
            );
        }
        catch (ImplementationError implError)
        {
            reportStatic(
                implError,
                "An implementation error occured during creation of resource '" + rscNameStr + "' on node '" +
                nodeNameStr + "'.",
                null, // cause
                null, // details
                null, // corerction
                ApiConsts.RC_RSC_CRT_FAIL_IMPL_ERROR,
                getObjRefs(nodeNameStr, rscNameStr),
                getVariables(nodeNameStr, rscNameStr),
                apiCallRc,
                controller,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    private NodeId getNextFreeNodeId(ResourceDefinitionData rscDfn)
    {
        // TODO: maybe use the poolAllocator for nodeId

        Iterator<Resource>  rscIterator;

        try
        {
            rscIterator = rscDfn.iterateResource(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "iterate the resources of resource definition '" + rscDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }

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
                    new LinStorException(
                        String.format(
                            "Could not find valid nodeId. Most likely because the maximum count (%d)" +
                                " is already reached",
                            NodeId.NODE_ID_MAX + 1
                        )
                    )
                );
            }
            nodeId = new NodeId(id);
        }
        catch (ValueOutOfRangeException valueOutOfRangeExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError("Found nodeId was invalid", valueOutOfRangeExc)
            );
        }
        return nodeId;
    }

    private void notifySatellites(AccessContext accCtx, ResourceData rsc, ApiCallRcImpl apiCallRc)
    {
        // TODO: replace this method with super.updateSatellites(Resource) when reworking this API
        try
        {
            // notify all peers that (at least one of) their resource has changed
            Iterator<Resource> rscIterator = rsc.getDefinition().iterateResource(accCtx);
            while (rscIterator.hasNext())
            {
                Resource currentRsc = rscIterator.next();
                Peer peer = currentRsc.getAssignedNode().getPeer(apiCtx);

                if (peer.isConnected())
                {
                    Message message = peer.createMessage();
                    byte[] data = rscSerializer.getChangedMessage(currentRsc);
                    message.setData(data);
                    peer.sendMessage(message);
                }
                else
                {
                    ApiCallRcEntry notConnected = new ApiCallRcEntry();
                    notConnected.setReturnCode(RC_RSC_CRT_WARN_NOT_CONNECTED);
                    String nodeName = currentRsc.getAssignedNode().getName().displayValue;
                    notConnected.setMessageFormat(
                        "No active connection to satellite '" + nodeName + "'"
                    );
                    notConnected.setDetailsFormat(
                        "The satellite was added and the controller tries to (re-) establish connection to it." +
                        "The controller stored the new Resource and as soon the satellite is connected, it will " +
                        "receive this update."
                    );
                    notConnected.putObjRef(ApiConsts.KEY_NODE, nodeName);
                    notConnected.putObjRef(ApiConsts.KEY_RSC_DFN, currentRsc.getDefinition().getName().displayValue);
                    notConnected.putVariable(ApiConsts.KEY_NODE_NAME, nodeName);
                    apiCallRc.addEntry(notConnected);
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to contact all satellites about a resource change",
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

    public ApiCallRc modifyResource(
        AccessContext accCtx,
        Peer client,
        UUID rscUuid,
        String nodeNameStr,
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
                null,
                nodeNameStr,
                rscNameStr
            );
        )
        {
            ResourceData rsc = loadRsc(nodeNameStr, rscNameStr);

            if (rscUuid != null && !rscUuid.equals(rsc.getUuid()))
            {
                addAnswer(
                    "UUID-check failed",
                    ApiConsts.FAIL_UUID_RSC
                );
                throw new ApiCallHandlerFailedException();
            }

            Props props = getProps(rsc);
            Map<String, String> propsMap = props.map();

            propsMap.putAll(overrideProps);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            notifySatellites(accCtx, rsc, apiCallRc); // TODO: update this when reworking APIs
            reportSuccess("Resource '" + rscNameStr + "' on node '" + nodeNameStr + "' modified.");
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
                "Modification of a node failed due to an unknown exception.",
                ApiConsts.FAIL_UNKNOWN_ERROR
            );
        }
        catch (ImplementationError implErr)
        {
            asImplError(implErr);
        }

        return apiCallRc;
    }

    private interface ExecuteDelete {
        void onSuccess(
            AccessContext accCtx,
            Peer client,
            String nodeNameStr,
            String rscNameStr,
            ResourceData rscData,
            TransactionMgr transMgr,
            ApiCallRcImpl apiCallRc
        ) throws AccessDeniedException, SQLException;
    }

    /**
     * checksForDeletion does all kind of checks to delete/markdelete a resource.
     * Like does the node exist, does the resource exist, access checks...
     *
     * @param accCtx
     * @param client
     * @param nodeNameStr
     * @param rscNameStr
     * @param execDel Basically a function pointer to execute code if everything went well.
     * @return The constructed ApiCall return code object
     */
    private ApiCallRc checksForDeletion(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr,
        ExecuteDelete execDel
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
                null, // secret only needed if we want to persist this entry
                null, // transportType only needed if we want to persist this entry
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
                rscNotFoundEntry.setReturnCode(RC_RSC_DEL_WARN_NOT_FOUND);
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
                // call the success action interface
                execDel.onSuccess(accCtx, client, nodeNameStr, rscNameStr, rscData, transMgr, apiCallRc);
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
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
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
            controller.dbConnPool.returnConnection(transMgr);
        }

        return apiCallRc;
    }

    public ApiCallRc deleteResource(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr
    )
    {
        ApiCallRc apiCallRc = checksForDeletion(accCtx, client, nodeNameStr, rscNameStr, new ExecuteDelete() {
            @Override
            public void onSuccess(
                    AccessContext accCtx,
                    Peer client,
                    String nodeNameStr,
                    String rscNameStr,
                    ResourceData rscData,
                    TransactionMgr transMgr,
                    ApiCallRcImpl apiCallRc) throws AccessDeniedException, SQLException
            {
                int volumeCount = rscData.getVolumeCount();
                rscData.setConnection(transMgr);
                String successMessage = "";
                if (volumeCount > 0)
                {
                    successMessage = String.format(
                        "Resource '%s' marked to be deleted from node '%s'.",
                        rscNameStr,
                        nodeNameStr
                    );
                    rscData.markDeleted(accCtx);
                }
                else
                {
                    successMessage = String.format(
                        "Resource '%s' is deleted from node '%s'.",
                        rscNameStr,
                        nodeNameStr
                    );
                    rscData.delete(accCtx);
                }
                transMgr.commit();

                if (volumeCount > 0)
                {
                    // notify satellites
                    updateSatellites(rscData);
                }

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DELETED);
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
        });

        return apiCallRc;
    }

    /**
     * This is called if a satellite has deleted its resource to notify the controller
     * that it can delete the resource.
     *
     * @param accCtx
     * @param client
     * @param nodeNameStr
     * @param rscNameStr
     * @return
     */
    public ApiCallRc resourceDeleted(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr
    )
    {
        ApiCallRc apiCallRc = checksForDeletion(accCtx, client, nodeNameStr, rscNameStr, new ExecuteDelete() {
            @Override
            public void onSuccess(
                    AccessContext accCtx,
                    Peer client,
                    String nodeNameStr,
                    String rscNameStr,
                    ResourceData rscData,
                    TransactionMgr transMgr,
                    ApiCallRcImpl apiCallRc) throws AccessDeniedException, SQLException
            {
                ResourceDefinition rscDfn = rscData.getDefinition();
                rscData.setConnection(transMgr);
                rscData.delete(accCtx);
                transMgr.commit();

                // call cleanup if resource definition is empty
                if (rscDfn.getResourceCount() == 0)
                {
                    controller.cleanup();
                }

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_RSC_DELETED);
                String successMessage = String.format(
                    "Resource '%s' is deleted from node '%s'.",
                    rscNameStr,
                    nodeNameStr
                );
                entry.setMessageFormat(successMessage);
                entry.putObjRef(KEY_NODE, nodeNameStr);
                entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                entry.putObjRef(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                apiCallRc.addEntry(entry);

                controller.getErrorReporter().logInfo(successMessage);
            }
        });

        return apiCallRc;
    }

    ApiCallRc volumeDeleted(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr,
        final int volumeNr
    )
    {
        ApiCallRc apiCallRc = checksForDeletion(accCtx, client, nodeNameStr, rscNameStr, new ExecuteDelete() {
            @Override
            public void onSuccess(
                    AccessContext accCtx,
                    Peer client,
                    String nodeNameStr,
                    String rscNameStr,
                    ResourceData rscData,
                    TransactionMgr transMgr,
                    ApiCallRcImpl apiCallRc) throws AccessDeniedException, SQLException
            {
                VolumeNumber volumeNumber = null;
                try {
                    volumeNumber = new VolumeNumber(volumeNr);
                } catch (ValueOutOfRangeException exc) {
                    String errorMessage = String.format(
                        "Volume number '%d' out of range",
                        volumeNr
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
                    entry.putObjRef(KEY_VLM_NR, Integer.toString(volumeNr));

                    apiCallRc.addEntry(entry);
                    return;
                }

                Volume vol = rscData.getVolume(volumeNumber);
                vol.delete(accCtx);

                transMgr.commit();

                //TODO check call cleanup??

                ApiCallRcEntry entry = new ApiCallRcEntry();
                entry.setReturnCodeBit(RC_VLM_DELETED);
                String successMessage = String.format(
                    "VolumeNr '%d' on resource '%s' is deleted from node '%s'.",
                    volumeNr,
                    rscNameStr,
                    nodeNameStr
                );
                entry.setMessageFormat(successMessage);
                entry.putObjRef(KEY_NODE, nodeNameStr);
                entry.putObjRef(KEY_RSC_DFN, rscNameStr);
                entry.putObjRef(KEY_NODE_NAME, nodeNameStr);
                entry.putVariable(KEY_RSC_NAME, rscNameStr);
                entry.putObjRef(KEY_VLM_NR, Integer.toString(volumeNr));
                apiCallRc.addEntry(entry);

                controller.getErrorReporter().logInfo(successMessage);
            }
        });

        return apiCallRc;
    }

    byte[] listResources(int msgId, AccessContext accCtx, Peer client)
    {
        ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
        try
        {
            controller.rscDfnMapProt.requireAccess(accCtx, AccessType.VIEW);// accDeniedExc1
            controller.nodesMapProt.requireAccess(accCtx, AccessType.VIEW);
            for (ResourceDefinition rscDfn : controller.rscDfnMap.values())
            {
                try
                {
                    Iterator<Resource> itResources = rscDfn.iterateResource(accCtx);
                    while (itResources.hasNext())
                    {
                        Resource rsc = itResources.next();
                        rscs.add(rsc.getApiData(accCtx));
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        try
        {
            return rscListSerializer.getListMessage(msgId, rscs);
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

    public void respondResource(
        int msgId,
        Peer satellitePeer,
        String nodeNameStr,
        UUID rscUuid,
        String rscNameStr
    )
    {
        try
        {
            NodeName nodeName = new NodeName(nodeNameStr);

            TransactionMgr transMgr = new TransactionMgr(controller.dbConnPool);
            NodeData node = NodeData.getInstance(
                apiCtx,
                nodeName,
                null,
                null,
                transMgr,
                false,
                false
            );
            if (node != null)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Resource rsc = node.getResource(apiCtx, rscName);
                // TODO: check if the localResource has the same uuid as rscUuid
                if (rsc != null)
                {
                    byte[] data = rscSerializer.getDataMessage(msgId, rsc);

                    Message response = satellitePeer.createMessage();
                    response.setData(data);
                    satellitePeer.sendMessage(response);
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

                    // satellite has divergent data. cut the connection, let reconnector task
                    // establish new connection and satellite will ask for a full resync.
                    satellitePeer.closeConnection();
                }
            }
            else
            {
                controller.getErrorReporter().reportError(
                    new ImplementationError(
                        "Satellite requested resource '" + rscNameStr + "' on node '" + nodeNameStr + "' " +
                            "but that node does not exist.",
                        null
                    )
                );
                satellitePeer.closeConnection();
            }
            transMgr.rollback(); // just to be sure
            controller.dbConnPool.returnConnection(transMgr);
        }
        catch (SQLException sqlExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Could not create a transMgr",
                    sqlExc
                )
            );
        }
        catch (InvalidNameException invalidNameExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite requested data for invalid name (node or rsc name).",
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
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            controller.getErrorReporter().reportError(
                new ImplementationError(
                    "Read-only NodeData.getInstance threw a dataAlreadyExistsException",
                    dataAlreadyExistsExc
                )
            );
        }
    }

    private AbsApiCallHandler setCurrent(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeNameStr,
        String rscNameStr
    )
    {
        super.setCurrent(accCtx, peer, type, apiCallRc, transMgr);
        currentNodeName.set(nodeNameStr);
        currentRscName.set(rscNameStr);

        Map<String, String> objRefs = currentObjRefs.get();
        objRefs.clear();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        Map<String, String> vars = currentVariables.get();
        vars.clear();
        vars.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        vars.put(ApiConsts.KEY_RSC_NAME, rscNameStr);

        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + currentNodeName.get() + ", Resource: " + currentRscName.get();
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return "resource '" + currentRscName.get() + "' on node '" + currentNodeName.get() + "'";
    }

    private Map<String, String> getObjRefs(String nodeNameStr, String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE, nodeNameStr);
        map.put(ApiConsts.KEY_RSC_DFN, rscNameStr);
        return map;
    }

    private Map<String, String> getVariables(String nodeNameStr, String rscNameStr)
    {
        Map<String, String> map = new TreeMap<>();
        map.put(ApiConsts.KEY_NODE_NAME, nodeNameStr);
        map.put(ApiConsts.KEY_RSC_NAME, rscNameStr);
        return map;
    }

    private ResourceData createResource(ResourceDefinitionData rscDfn, NodeData node, NodeId nodeId)
    {
        try
        {
            return ResourceData.getInstance(
                currentAccCtx.get(),
                rscDfn,
                node,
                nodeId,
                null, // flags
                currentTransMgr.get(),
                true, // persist this entry
                true // throw exception if the entry exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create the " + getObjectDescriptionInline() + ".",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating the " + getObjectDescriptionInline()
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                "A " + getObjectDescriptionInline() + " already exists.",
                ApiConsts.FAIL_EXISTS_RSC
            );
        }
    }

    private ResourceData loadRsc(String nodeName, String rscName) throws ApiCallHandlerFailedException
    {
        Node node = loadNode(nodeName, true);
        ResourceDefinitionData rscDfn = loadRscDfn(rscName, true);

        try
        {
            return ResourceData.getInstance(
                currentAccCtx.get(),
                rscDfn,
                node,
                null,
                null,
                currentTransMgr.get(),
                false,
                false
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'.",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ImplementationError(
                "Loading a resource caused DataAlreadyExistsException",
                dataAlreadyExistsExc
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading resource '" + rscName + "' on node '" + nodeName + "'."
            );
        }
    }

    private VolumeData createVolume(
        Resource rsc,
        VolumeDefinition vlmDfn,
        StorPool storPool,
        VlmApi vlmApi
    )
    {
        try
        {
            String blockDevice = vlmApi == null ? null : vlmApi.getBlockDevice();
            String metaDisk = vlmApi == null ? null : vlmApi.getMetaDisk();

            return VolumeData.getInstance(
                currentAccCtx.get(),
                rsc,
                vlmDfn,
                storPool,
                blockDevice,
                metaDisk,
                null, // flags
                currentTransMgr.get(),
                true, // persist this entry
                true // throw exception if the entry exists
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "create " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw asExc(
                dataAlreadyExistsExc,
                "The " + getObjectDescriptionInline() + " already exists",
                ApiConsts.FAIL_EXISTS_VLM
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "creating " + getObjectDescriptionInline()
            );
        }
    }

    private Iterator<VolumeDefinition> getVlmDfnIterator(ResourceDefinitionData rscDfn)
    {
        try
        {
            return rscDfn.iterateVolumeDfn(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }

    }
}
