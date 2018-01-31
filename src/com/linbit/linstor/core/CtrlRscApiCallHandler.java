package com.linbit.linstor.core;

import static com.linbit.linstor.api.ApiConsts.API_LST_RSC;
import static com.linbit.linstor.api.ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_NAME;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeId;
import com.linbit.linstor.NodeIdAlloc;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.RscDfnFlags;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.VolumeData;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.FlagsHelper;
import java.util.Arrays;

class CtrlRscApiCallHandler extends AbsApiCallHandler
{
    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentRscName = new ThreadLocal<>();
    private final CtrlClientSerializer clientComSerializer;

    CtrlRscApiCallHandler(
        ApiCtrlAccessors apiCtrlAccessorsRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        AccessContext apiCtxRef
    )
    {
        super (
            apiCtrlAccessorsRef,
            apiCtxRef,
            ApiConsts.MASK_RSC,
            interComSerializer
        );
        super.setNullOnAutoClose(
            currentNodeName,
            currentRscName
        );
        clientComSerializer = clientComSerializerRef;
    }

    public ApiCallRc createResource(
        AccessContext accCtx,
        Peer client,
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<VlmApi> vlmApiList
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();


        try (
            AbsApiCallHandler basicallyThis = setContext(
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

            ResourceData rsc = createResource(rscDfn, node, nodeId, flagList);
            Props rscProps = getProps(rsc);
            rscProps.map().putAll(rscPropsMap);

            boolean isRscDiskless = isDiskless(rsc);

            if (isRscDiskless)
            {
                rscProps.setProp(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
            }

            Props rscDfnProps = getProps(rscDfn);
            Props nodeProps = getProps(node);

            Map<Integer, Volume> vlmMap = new TreeMap<>();
            for (VlmApi vlmApi : vlmApiList)
            {
                VolumeDefinitionData vlmDfn = loadVlmDfn(rscDfn, vlmApi.getVlmNr(), true);

                StorPoolData storPool;
                if (isRscDiskless)
                {
                    storPool = (StorPoolData) node.getDisklessStorPool(accCtx);
                }
                else
                {
                    String storPoolNameStr;
                    storPoolNameStr = vlmApi.getStorPoolName();
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
                        storPoolNameStr = apiCtrlAccessors.getDefaultStorPoolName();
                    }
                    StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr, true);
                    storPool = loadStorPool(storPoolDfn, node, true);
                }

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

                    String storPoolNameStr;
                    StorPool storPool;
                    if (isRscDiskless)
                    {
                        storPool = rsc.getAssignedNode().getDisklessStorPool(apiCtx);
                        storPoolNameStr = LinStor.DISKLESS_STOR_POOL_NAME;
                    }
                    else
                    {
                        storPoolNameStr = prioProps.getProp(KEY_STOR_POOL_NAME);
                        if (storPoolNameStr == null || "".equals(storPoolNameStr))
                        {
                            storPoolNameStr = apiCtrlAccessors.getDefaultStorPoolName();
                        }
                        storPool = rsc.getAssignedNode().getStorPool(
                            apiCtx,
                            asStorPoolName(storPoolNameStr)
                        );
                    }

                    if (storPool == null)
                    {
                        throw asExc(
                            new LinStorException("Dependency not found"),
                            "The default storage pool '" + storPoolNameStr + "' " +
                            "for resource '" + rsc.getDefinition().getName().displayValue + "' " +
                            "for volume number '" +  vlmDfn.getVolumeNumber().value + "' " +
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
                        VolumeData vlm = createVolume(rsc, vlmDfn, storPool, null);
                        vlmMap.put(vlmDfn.getVolumeNumber().value, vlm);
                    }
                }
            }

            commit();

            if (rsc.getVolumeCount() > 0)
            {
                // only notify satellite if there are volumes to deploy.
                // otherwise a bug occurs when an empty resource is deleted
                // the controller instantly deletes it (without marking for deletion first)
                // but doesn't tell the satellite...
                // the next time a resource with the same name will get a different UUID and
                // will cause a conflict (and thus, an exception) on the satellite
                updateSatellites(rsc);
            }
            // TODO: if a satellite confirms creation, also log it to controller.info

            reportSuccess(rsc.getUuid());

            for (Entry<Integer, Volume> entry : vlmMap.entrySet())
            {
                ApiCallRcEntry vlmCreatedRcEntry = new ApiCallRcEntry();
                vlmCreatedRcEntry.setMessageFormat(
                    "Volume with number '" + entry.getKey() + "' on resource '" +
                        entry.getValue().getResourceDefinition().getName().displayValue + "' on node '" +
                        entry.getValue().getResource().getAssignedNode().getName().displayValue + "' successfully created"
                );
                vlmCreatedRcEntry.setDetailsFormat(
                    "Volume UUID is: " + entry.getValue().getUuid().toString()
                );
                vlmCreatedRcEntry.setReturnCode(ApiConsts.MASK_VLM | ApiConsts.CREATED);
                vlmCreatedRcEntry.putAllObjRef(currentObjRefs.get());
                vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(entry.getKey()));
                vlmCreatedRcEntry.putAllVariables(currentVariables.get());
                vlmCreatedRcEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(entry.getKey()));

                apiCallRc.addEntry(vlmCreatedRcEntry);
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
                getObjectDescriptionInline(nodeNameStr, rscNameStr),
                getObjRefs(nodeNameStr, rscNameStr),
                getVariables(nodeNameStr, rscNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    private boolean isDiskless(ResourceData rsc)
    {
        try
        {
            return rsc.getStateFlags().isSet(apiCtx, RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw asImplError(implError);
        }
    }

    private NodeId getNextFreeNodeId(ResourceDefinitionData rscDfn)
    {
        NodeId freeNodeId;
        try
        {
            Iterator<Resource> rscIterator = rscDfn.iterateResource(currentAccCtx.get());
            int[] occupiedIds = new int[rscDfn.getResourceCount()];
            int idx = 0;
            while (rscIterator.hasNext())
            {
                occupiedIds[idx] = rscIterator.next().getNodeId().value;
                ++idx;
            }
            Arrays.sort(occupiedIds);

            freeNodeId = NodeIdAlloc.getFreeNodeId(occupiedIds);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "iterate the resources of resource definition '" + rscDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (ExhaustedPoolException exhaustedPoolExc)
        {
            throw asExc(
                exhaustedPoolExc,
                "An exception occured during generation of a node id.",
                ApiConsts.FAIL_POOL_EXHAUSTED_NODE_ID
            );
        }
        return freeNodeId;
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
            AbsApiCallHandler basicallyThis = setContext(
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
            ResourceData rsc = loadRsc(nodeNameStr, rscNameStr, true);

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

            updateSatellites(rsc);
            reportSuccess(rsc.getUuid());
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
                getObjectDescriptionInline(nodeNameStr, rscNameStr),
                getObjRefs(nodeNameStr, rscNameStr),
                getVariables(nodeNameStr, rscNameStr),
                apiCallRc,
                accCtx,
                client
            );
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
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null, // create new transMgr
                nodeNameStr,
                rscNameStr
            );
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, true);

            int volumeCount = rscData.getVolumeCount();
            String successMessage;
            String details;
            if (volumeCount > 0)
            {
                successMessage = getObjectDescriptionInlineFirstLetterCaps() + " marked for deletion.";
                details = getObjectDescriptionInlineFirstLetterCaps() + " UUID is: " + rscData.getUuid();
                markDeleted(rscData);
            }
            else
            {
                successMessage = getObjectDescriptionInlineFirstLetterCaps() + " deleted.";
                details = getObjectDescriptionInlineFirstLetterCaps() + " UUID was: " + rscData.getUuid();
                delete(rscData);
            }

            commit();

            if (volumeCount > 0)
            {
                // notify satellites
                updateSatellites(rscData);
            }

            reportSuccess(successMessage, details);
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
                getObjectDescriptionInline(nodeNameStr, rscNameStr),
                getObjRefs(nodeNameStr, rscNameStr),
                getVariables(nodeNameStr, rscNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }


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
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                accCtx,
                client,
                ApiCallType.DELETE,
                apiCallRc,
                null, // create new transMgr
                nodeNameStr,
                rscNameStr
            );
        )
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, false);

            if (rscData == null)
            {
                addAnswer(
                    getObjectDescriptionInlineFirstLetterCaps() + " not found",
                    ApiConsts.WARN_NOT_FOUND
                );
                throw new ApiCallHandlerFailedException();
            }

            ResourceDefinition rscDfn = rscData.getDefinition();
            Node node = rscData.getAssignedNode();
            UUID rscUuid = rscData.getUuid();

            delete(rscData); // also deletes all of its volumes

            UUID rscDfnUuid = null;
            ResourceName deletedRscDfnName = null;
            UUID nodeUuid = null;
            NodeName deletedNodeName = null;
            // cleanup resource definition if empty and marked for deletion
            if (rscDfn.getResourceCount() == 0 && isMarkedForDeletion(rscDfn))
            {
                deletedRscDfnName = rscDfn.getName();
                rscDfnUuid = rscDfn.getUuid();
                delete(rscDfn);
            }

            // cleanup node if empty and marked for deletion
            if (node.getResourceCount() == 0 &&
                isMarkedForDeletion(node)
            )
            {
                // TODO check if the remaining storage pools have deployed values left (impl error)


                deletedNodeName = node.getName();
                nodeUuid = node.getUuid();
                delete(node);
            }

            commit();

            reportSuccess(rscUuid);

            if (deletedRscDfnName != null)
            {
                addRscDfnDeletedAnswer(deletedRscDfnName, rscDfnUuid);
                apiCtrlAccessors.getRscDfnMap().remove(deletedRscDfnName);
            }
            if (deletedNodeName != null)
            {
                apiCtrlAccessors.getNodesMap().remove(deletedNodeName);
                addNodeDeletedAnswer(deletedNodeName, nodeUuid);
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
                ApiCallType.MODIFY,
                getObjectDescriptionInline(nodeNameStr, rscNameStr),
                getObjRefs(nodeNameStr, rscNameStr),
                getVariables(nodeNameStr, rscNameStr),
                apiCallRc,
                accCtx,
                client
            );
        }

        return apiCallRc;
    }

    byte[] listResources(int msgId, AccessContext accCtx, Peer client)
    {
        ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
        List<ResourceState> rscStates = new ArrayList<>();
        try
        {
            apiCtrlAccessors.getRscDfnMapProtection().requireAccess(accCtx, AccessType.VIEW);// accDeniedExc1
            apiCtrlAccessors.getNodesMapProtection().requireAccess(accCtx, AccessType.VIEW);
            for (ResourceDefinition rscDfn : apiCtrlAccessors.getRscDfnMap().values())
            {
                try
                {
                    Iterator<Resource> itResources = rscDfn.iterateResource(accCtx);
                    while (itResources.hasNext())
                    {
                        Resource rsc = itResources.next();
                        rscs.add(rsc.getApiData(accCtx, null, null));
                        // fullSyncId and updateId null, as they are not going to be serialized anyways

                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }

            // get resource states of all nodes
            for (final Node node : apiCtrlAccessors.getNodesMap().values())
            {
                final Peer peer = node.getPeer(accCtx);
                if (peer != null)
                {
                    final Map<ResourceName, ResourceState> resourceStateMap = peer.getResourceStates();

                    if (resourceStateMap != null) {
                        ArrayList<ResourceState> stateCopy = new ArrayList<>(resourceStateMap.values());
                        for (ResourceState rscState : stateCopy) {
                            rscState.setNodeName(node.getName().getDisplayName());
                            rscStates.add(rscState);
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
            apiCtrlAccessors.getErrorReporter().reportError(accDeniedExc);
        }

        return clientComSerializer
                .builder(API_LST_RSC, msgId)
                .resourceList(rscs, rscStates)
                .build();
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

            Node node = apiCtrlAccessors.getNodesMap().get(nodeName);

            if (node != null)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Resource rsc = node.getResource(apiCtx, rscName);
                // TODO: check if the localResource has the same uuid as rscUuid
                if (rsc != null)
                {
                    long fullSyncTimestamp = satellitePeer.getFullSyncId();
                    long updateId = satellitePeer.getNextSerializerId();

                    byte[] data = internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_RSC, msgId)
                        .resourceData(rsc, fullSyncTimestamp, updateId)
                        .build();

                    Message response = satellitePeer.createMessage();
                    response.setData(data);
                    satellitePeer.sendMessage(response);
                }
                else
                {
                    apiCtrlAccessors.getErrorReporter().reportError(
                        new ImplementationError(
                            String.format(
                                "A requested resource name '%s' with the uuid '%s' was not found "+
                                    "in the controller's list of resources",
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
                apiCtrlAccessors.getErrorReporter().reportError(
                    new ImplementationError(
                        "Satellite requested resource '" + rscNameStr + "' on node '" + nodeNameStr + "' " +
                            "but that node does not exist.",
                        null
                    )
                );
                satellitePeer.closeConnection();
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Satellite requested data for invalid name (node or rsc name).",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested resource data.",
                    accDeniedExc
                )
            );
        }
        catch (IllegalMessageStateException illegalMessageStateExc)
        {
            apiCtrlAccessors.getErrorReporter().reportError(
                new ImplementationError(
                    "Failed to respond to resource data request",
                    illegalMessageStateExc
                )
            );
        }
    }

    private AbsApiCallHandler setContext(
        AccessContext accCtx,
        Peer peer,
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        TransactionMgr transMgr,
        String nodeNameStr,
        String rscNameStr
    )
    {
        super.setContext(
            accCtx,
            peer,
            type,
            apiCallRc,
            transMgr,
            getObjRefs(nodeNameStr, rscNameStr),
            getVariables(nodeNameStr, rscNameStr)
        );
        currentNodeName.set(nodeNameStr);
        currentRscName.set(rscNameStr);
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
        return getObjectDescriptionInline(currentNodeName.get(), currentRscName.get());
    }

    private String getObjectDescriptionInline(String nodeNameStr, String rscNameStr)
    {
        return "resource '" + rscNameStr + "' on node '" + nodeNameStr + "'";
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

    private ResourceData createResource(
        ResourceDefinitionData rscDfn,
        NodeData node,
        NodeId nodeId,
        List<String> flagList
    )
    {
        RscFlags[] flags = RscFlags.restoreFlags(
            FlagsHelper.fromStringList(
                RscFlags.class,
                flagList
            )
        );
        try
        {
            return ResourceData.getInstance(
                currentAccCtx.get(),
                rscDfn,
                node,
                nodeId,
                flags,
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

    protected final VolumeDefinitionData loadVlmDfn(
        ResourceDefinitionData rscDfn,
        int vlmNr,
        boolean failIfNull
    )
        throws ApiCallHandlerFailedException
    {
        return loadVlmDfn(rscDfn, asVlmNr(vlmNr), failIfNull);
    }

    protected final VolumeDefinitionData loadVlmDfn(
        ResourceDefinitionData rscDfn,
        VolumeNumber vlmNr,
        boolean failIfNull
    )
        throws ApiCallHandlerFailedException
    {
        try
        {
            VolumeDefinitionData vlmDfn = VolumeDefinitionData.getInstance(
                currentAccCtx.get(),
                rscDfn,
                vlmNr,
                null, // minor
                null, // volsize
                null, // flags
                currentTransMgr.get(),
                false, // do not create
                false // do not fail if exists
            );

            if (failIfNull && vlmDfn == null)
            {
                throw asExc(
                    null,
                    "Volume definition with number '" + vlmNr.value + "' on resource definition '" + rscDfn.getName().displayValue + "' not found.",
                    "The specified volume definition with number '" + vlmNr.value + "' on resource definition '" + rscDfn.getName().displayValue + "' could not be found in the database",
                    null, // details
                    "Create a volume definition with number '" + vlmNr.value + "' on resource definition '" + rscDfn.getName().displayValue + "' first.",
                    ApiConsts.FAIL_NOT_FOUND_VLM_DFN
                );
            }

            return vlmDfn;
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "load " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException | MdException implErr)
        {
            throw asImplError(implErr);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading " + getObjectDescriptionInline()
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

    protected final Props getProps(Resource rsc) throws ApiCallHandlerFailedException
    {
        try
        {
            return rsc.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties for resource '" + rsc.getDefinition().getName().displayValue + "' on node '" +
                rsc.getAssignedNode().getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
    }

    protected final Props getProps(Volume vlm) throws ApiCallHandlerFailedException
    {
        try
        {
            return vlm.getProps(currentAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties for volume with number '" + vlm.getVolumeDefinition().getVolumeNumber().value + "' " +
                "on resource '" + vlm.getResourceDefinition().getName().displayValue + "' " +
                "on node '" + vlm.getResource().getAssignedNode().getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
    }

    private void markDeleted(ResourceData rscData)
    {
        try
        {
            rscData.markDeleted(currentAccCtx.get());
            Iterator<Volume> volumesIterator = rscData.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(currentAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "mark " + getObjectDescriptionInline() + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "marking " + getObjectDescriptionInline() + " as deleted"
            );
        }
    }

    private void delete(ResourceData rscData)
    {
        try
        {
            rscData.delete(currentAccCtx.get()); // also deletes all of its volumes
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "delete " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + getObjectDescriptionInline()
            );
        }
    }

    private void delete(ResourceDefinition rscDfn)
    {
        try
        {
            rscDfn.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + CtrlRscDfnApiCallHandler.getObjectDescriptionInline(rscDfn.getName().displayValue)
            );
        }
    }

    private void delete(Node node)
    {
        try
        {
            node.delete(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "deleting " + CtrlNodeApiCallHandler.getObjectDescriptionInline(node.getName().displayValue)
            );
        }
    }

    private boolean isMarkedForDeletion(ResourceDefinition rscDfn)
    {
        try
        {
            return rscDfn.getFlags().isSet(apiCtx, RscDfnFlags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private boolean isMarkedForDeletion(Node node)
    {
        try
        {
            return node.getFlags().isSet(apiCtx, NodeFlag.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
    }

    private void addRscDfnDeletedAnswer(ResourceName rscName, UUID rscDfnUuid)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();

        String rscDeletedMsg = CtrlRscDfnApiCallHandler.getObjectDescriptionInline(rscName.displayValue) +
            " deleted.";
        rscDeletedMsg = rscDeletedMsg.substring(0, 1).toUpperCase() + rscDeletedMsg.substring(1);
        entry.setMessageFormat(rscDeletedMsg);
        entry.setReturnCode(ApiConsts.MASK_RSC_DFN | ApiConsts.DELETED);
        entry.putObjRef(ApiConsts.KEY_RSC_DFN, rscName.displayValue);
        entry.putObjRef(ApiConsts.KEY_UUID, rscDfnUuid.toString());
        entry.putVariable(ApiConsts.KEY_RSC_NAME, rscName.displayValue);

        currentApiCallRc.get().addEntry(entry);
        apiCtrlAccessors.getErrorReporter().logInfo(rscDeletedMsg);
    }

    private void addNodeDeletedAnswer(NodeName nodeName, UUID nodeUuid)
    {
        ApiCallRcEntry entry = new ApiCallRcEntry();

        String rscDeletedMsg = CtrlNodeApiCallHandler.getObjectDescriptionInline(nodeName.displayValue) +
            " deleted.";
        entry.setMessageFormat(rscDeletedMsg);
        entry.setReturnCode(ApiConsts.MASK_NODE | ApiConsts.DELETED);
        entry.putObjRef(ApiConsts.KEY_NODE, nodeName.displayValue);
        entry.putObjRef(ApiConsts.KEY_UUID, nodeUuid.toString());
        entry.putVariable(ApiConsts.KEY_NODE_NAME, nodeName.displayValue);

        currentApiCallRc.get().addEntry(entry);
        apiCtrlAccessors.getErrorReporter().logInfo(rscDeletedMsg);
    }
}
