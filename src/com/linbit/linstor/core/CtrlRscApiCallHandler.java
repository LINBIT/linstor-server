package com.linbit.linstor.core;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinStorRuntimeException;
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
import com.linbit.linstor.ResourceDataFactory;
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
import com.linbit.linstor.VolumeDataFactory;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeDefinitionDataControllerFactory;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static com.linbit.linstor.api.ApiConsts.API_LST_RSC;
import static com.linbit.linstor.api.ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_NAME;

public class CtrlRscApiCallHandler extends AbsApiCallHandler
{
    private String nodeName;
    private String rscName;
    private final CtrlClientSerializer clientComSerializer;
    private final ObjectProtection rscDfnMapProt;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection nodesMapProt;
    private final CoreModule.NodesMap nodesMap;
    private final String defaultStorPoolName;
    private final ResourceDataFactory resourceDataFactory;
    private final VolumeDataFactory volumeDataFactory;
    private final VolumeDefinitionDataControllerFactory volumeDefinitionDataFactory;

    @Inject
    public CtrlRscApiCallHandler(
        ErrorReporter errorReporterRef,
        CtrlStltSerializer interComSerializer,
        CtrlClientSerializer clientComSerializerRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(ControllerSecurityModule.RSC_DFN_MAP_PROT) ObjectProtection rscDfnMapProtRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        @Named(ControllerSecurityModule.NODES_MAP_PROT) ObjectProtection nodesMapProtRef,
        CoreModule.NodesMap nodesMapRef,
        @Named(ConfigModule.CONFIG_STOR_POOL_NAME) String defaultStorPoolNameRef,
        CtrlObjectFactories objectFactories,
        ResourceDataFactory resourceDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext AccessContext peerAccCtxRef,
        Peer peerRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            ApiConsts.MASK_RSC,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef
        );
        clientComSerializer = clientComSerializerRef;
        rscDfnMapProt = rscDfnMapProtRef;
        rscDfnMap = rscDfnMapRef;
        nodesMapProt = nodesMapProtRef;
        nodesMap = nodesMapRef;
        defaultStorPoolName = defaultStorPoolNameRef;
        resourceDataFactory = resourceDataFactoryRef;
        volumeDataFactory = volumeDataFactoryRef;
        volumeDefinitionDataFactory = volumeDefinitionDataFactoryRef;
    }

    public ApiCallRc createResource(
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<VlmApi> vlmApiList
    )
    {
        return createResource(
            nodeNameStr,
            rscNameStr,
            flagList,
            rscPropsMap,
            vlmApiList,
            true,
            null
        );
    }

    public ApiCallRc createResource(
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<VlmApi> vlmApiList,
        boolean autoCloseCurrentTransMgr,
        ApiCallRcImpl apiCallRcRef
    )
    {
        ApiCallRcImpl apiCallRc = apiCallRcRef;
        if (apiCallRc == null)
        {
            apiCallRc = new ApiCallRcImpl();
        }

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.CREATE,
                apiCallRc,
                autoCloseCurrentTransMgr,
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
                    storPool = (StorPoolData) node.getDisklessStorPool(peerAccCtx);
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
                        storPoolNameStr = defaultStorPoolName;
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

                objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));
                variables.put(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

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
                            storPoolNameStr = defaultStorPoolName;
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
                        entry.getValue().getResource().getAssignedNode().getName().displayValue +
                        "' successfully created"
                );
                vlmCreatedRcEntry.setDetailsFormat(
                    "Volume UUID is: " + entry.getValue().getUuid().toString()
                );
                vlmCreatedRcEntry.setReturnCode(ApiConsts.MASK_CRT | ApiConsts.MASK_VLM | ApiConsts.CREATED);
                vlmCreatedRcEntry.putAllObjRef(objRefs);
                vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(entry.getKey()));
                vlmCreatedRcEntry.putAllVariables(variables);
                vlmCreatedRcEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(entry.getKey()));

                apiCallRc.addEntry(vlmCreatedRcEntry);
            }
        }
        catch (ApiCallHandlerFailedException apiCallHandlerFailedExc)
        {
            // a report and a corresponding api-response already created.
            // however, if the autoCloseCurrentTransMgr was set to false, we are in the scope of an
            // other api call. because of that we re-throw this exception
            if (!autoCloseCurrentTransMgr)
            {
                throw apiCallHandlerFailedExc;
            }
        }
        catch (Exception | ImplementationError exc)
        {
            if (!autoCloseCurrentTransMgr)
            {
                if (exc instanceof ImplementationError)
                {
                    throw (ImplementationError) exc;
                }
                else
                {
                    throw new LinStorRuntimeException("Unknown Exception", exc);
                }
            }
            else
            {
                reportStatic(
                    exc,
                    ApiCallType.CREATE,
                    getObjectDescriptionInline(nodeNameStr, rscNameStr),
                    getObjRefs(nodeNameStr, rscNameStr),
                    getVariables(nodeNameStr, rscNameStr),
                    apiCallRc
                );
            }
        }

        return apiCallRc;
    }

    private boolean isDiskless(ResourceData rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw asImplError(implError);
        }
        return isDiskless;
    }

    private NodeId getNextFreeNodeId(ResourceDefinitionData rscDfn)
    {
        NodeId freeNodeId;
        try
        {
            Iterator<Resource> rscIterator = rscDfn.iterateResource(peerAccCtx);
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
                ApiCallType.MODIFY,
                apiCallRc,
                true, // autoClose
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
                apiCallRc
            );
        }

        return apiCallRc;
    }

    public ApiCallRc deleteResource(
        String nodeNameStr,
        String rscNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                true, // autoClose
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
                apiCallRc
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
        String nodeNameStr,
        String rscNameStr
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try (
            AbsApiCallHandler basicallyThis = setContext(
                ApiCallType.DELETE,
                apiCallRc,
                true, // autoClose
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
                rscDfnMap.remove(deletedRscDfnName);
            }
            if (deletedNodeName != null)
            {
                nodesMap.remove(deletedNodeName);
                node.getPeer(apiCtx).closeConnection();
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
                apiCallRc
            );
        }

        return apiCallRc;
    }

    byte[] listResources(int msgId)
    {
        ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
        List<ResourceState> rscStates = new ArrayList<>();
        try
        {
            rscDfnMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            nodesMapProt.requireAccess(peerAccCtx, AccessType.VIEW);
            for (ResourceDefinition rscDfn : rscDfnMap.values())
            {
                try
                {
                    Iterator<Resource> itResources = rscDfn.iterateResource(peerAccCtx);
                    while (itResources.hasNext())
                    {
                        Resource rsc = itResources.next();
                        rscs.add(rsc.getApiData(peerAccCtx, null, null));
                        // fullSyncId and updateId null, as they are not going to be serialized anyways

                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }

            // get resource states of all nodes
            for (final Node node : nodesMap.values())
            {
                final Peer peer = node.getPeer(peerAccCtx);
                if (peer != null)
                {
                    final Map<ResourceName, ResourceState> resourceStateMap = peer.getResourceStates();

                    if (resourceStateMap != null)
                    {
                        ArrayList<ResourceState> stateCopy = new ArrayList<>(resourceStateMap.values());
                        for (ResourceState rscState : stateCopy)
                        {
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
            errorReporter.reportError(accDeniedExc);
        }

        return clientComSerializer
                .builder(API_LST_RSC, msgId)
                .resourceList(rscs, rscStates)
                .build();
    }

    public void respondResource(
        int msgId,
        String nodeNameStr,
        UUID rscUuid,
        String rscNameStr
    )
    {
        try
        {
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = nodesMap.get(nodeName);

            if (node != null)
            {
                ResourceName rscName = new ResourceName(rscNameStr);
                Resource rsc = node.getResource(apiCtx, rscName);
                // TODO: check if the localResource has the same uuid as rscUuid
                if (rsc != null)
                {
                    long fullSyncTimestamp = peer.getFullSyncId();
                    long updateId = peer.getNextSerializerId();

                    peer.sendMessage(
                        internalComSerializer
                            .builder(InternalApiConsts.API_APPLY_RSC, msgId)
                            .resourceData(rsc, fullSyncTimestamp, updateId)
                            .build()
                    );
                }
                else
                {
                    peer.sendMessage(
                        internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_RSC_DELETED, msgId)
                        .deletedResourceData(rscNameStr)
                        .build()
                    );
                }
            }
            else
            {
                errorReporter.reportError(
                    new ImplementationError(
                        "Satellite requested resource '" + rscNameStr + "' on node '" + nodeNameStr + "' " +
                            "but that node does not exist.",
                        null
                    )
                );
                peer.closeConnection();
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid name (node or rsc name).",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested resource data.",
                    accDeniedExc
                )
            );
        }
    }

    private AbsApiCallHandler setContext(
        ApiCallType type,
        ApiCallRcImpl apiCallRc,
        boolean autoCloseCurrentTransMgr,
        String nodeNameStr,
        String rscNameStr
    )
    {
        super.setContext(
            type,
            apiCallRc,
            autoCloseCurrentTransMgr,
            getObjRefs(nodeNameStr, rscNameStr),
            getVariables(nodeNameStr, rscNameStr)
        );
        nodeName = nodeNameStr;
        rscName = rscNameStr;
        return this;
    }

    @Override
    protected String getObjectDescription()
    {
        return "Node: " + nodeName + ", Resource: " + rscName;
    }

    @Override
    protected String getObjectDescriptionInline()
    {
        return getObjectDescriptionInline(nodeName, rscName);
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
        ResourceData rsc;
        try
        {
            rsc = resourceDataFactory.getInstance(
                peerAccCtx,
                rscDfn,
                node,
                nodeId,
                flags,
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
        return rsc;
    }

    private VolumeData createVolume(
        Resource rsc,
        VolumeDefinition vlmDfn,
        StorPool storPool,
        VlmApi vlmApi
    )
    {
        VolumeData vlm;
        try
        {
            String blockDevice = vlmApi == null ? null : vlmApi.getBlockDevice();
            String metaDisk = vlmApi == null ? null : vlmApi.getMetaDisk();

            vlm = volumeDataFactory.getInstance(
                peerAccCtx,
                rsc,
                vlmDfn,
                storPool,
                blockDevice,
                metaDisk,
                null, // flags
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
        return vlm;
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
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = volumeDefinitionDataFactory.load(
                peerAccCtx,
                rscDfn,
                vlmNr
            );

            if (failIfNull && vlmDfn == null)
            {
                String rscName = rscDfn.getName().displayValue;
                throw asExc(
                    null,
                    "Volume definition with number '" + vlmNr.value + "' on resource definition '" +
                        rscName + "' not found.",
                    "The specified volume definition with number '" + vlmNr.value + "' on resource definition '" +
                        rscName + "' could not be found in the database",
                    null, // details
                    "Create a volume definition with number '" + vlmNr.value + "' on resource definition '" +
                        rscName + "' first.",
                    ApiConsts.FAIL_NOT_FOUND_VLM_DFN
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "load " + getObjectDescriptionInline(),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw asSqlExc(
                sqlExc,
                "loading " + getObjectDescriptionInline()
            );
        }
        return vlmDfn;
    }

    private Iterator<VolumeDefinition> getVlmDfnIterator(ResourceDefinitionData rscDfn)
    {
        Iterator<VolumeDefinition> iterator;
        try
        {
            iterator = rscDfn.iterateVolumeDfn(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return iterator;
    }

    protected final Props getProps(Resource rsc) throws ApiCallHandlerFailedException
    {
        Props props;
        try
        {
            props = rsc.getProps(peerAccCtx);
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
        return props;
    }

    protected final Props getProps(Volume vlm) throws ApiCallHandlerFailedException
    {
        Props props;
        try
        {
            props = vlm.getProps(peerAccCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asAccDeniedExc(
                accDeniedExc,
                "access properties for volume with number '" + vlm.getVolumeDefinition().getVolumeNumber().value +
                    "' on resource '" + vlm.getResourceDefinition().getName().displayValue + "' " +
                "on node '" + vlm.getResource().getAssignedNode().getName().displayValue + "'.",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        return props;
    }

    private void markDeleted(ResourceData rscData)
    {
        try
        {
            rscData.markDeleted(peerAccCtx);
            Iterator<Volume> volumesIterator = rscData.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(peerAccCtx);
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
            rscData.delete(peerAccCtx); // also deletes all of its volumes
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
        boolean isMarkedForDeletion;
        try
        {
            isMarkedForDeletion = rscDfn.getFlags().isSet(apiCtx, RscDfnFlags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return isMarkedForDeletion;
    }

    private boolean isMarkedForDeletion(Node node)
    {
        boolean isMarkedForDeletion;
        try
        {
            isMarkedForDeletion = node.getFlags().isSet(apiCtx, NodeFlag.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw asImplError(accDeniedExc);
        }
        return isMarkedForDeletion;
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

        apiCallRc.addEntry(entry);
        errorReporter.logInfo(rscDeletedMsg);
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

        apiCallRc.addEntry(entry);
        errorReporter.logInfo(rscDeletedMsg);
    }
}
