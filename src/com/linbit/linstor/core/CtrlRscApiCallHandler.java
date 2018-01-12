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
import com.linbit.drbd.md.MdException;
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
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.interfaces.serializer.CtrlSerializer;
import com.linbit.linstor.api.interfaces.serializer.InterComSerializer;
import com.linbit.linstor.netcom.IllegalMessageStateException;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import java.util.ArrayList;

class CtrlRscApiCallHandler extends AbsApiCallHandler
{
    private final CtrlSerializer<Resource> rscSerializer;
    private final InterComSerializer interComSerializer;

    private final ThreadLocal<String> currentNodeName = new ThreadLocal<>();
    private final ThreadLocal<String> currentRscName = new ThreadLocal<>();

    CtrlRscApiCallHandler(
        Controller controllerRef,
        CtrlSerializer<Resource> rscSerializerRef,
        InterComSerializer interComSerializerRef,
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
        interComSerializer = interComSerializerRef;
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
                vlmCreatedRcEntry.setReturnCode(ApiConsts.RC_VLM_CREATED);
                vlmCreatedRcEntry.putAllObjRef(currentObjRefs.get());
                vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(entry.getKey()));
                vlmCreatedRcEntry.putAllVariables(currentVariables.get());
                vlmCreatedRcEntry.putVariable(ApiConsts.KEY_VLM_NR, Integer.toString(entry.getKey()));

                apiCallRc.addEntry(vlmCreatedRcEntry);
            }
            updateSatellites(rsc);
            // TODO: if a satellite confirms creation, also log it to controller.info
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

            reportSuccess(rsc.getUuid());
            updateSatellites(rsc);
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
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr);

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
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr);

            ResourceDefinition rscDfn = rscData.getDefinition();
            delete(rscData);
            commit();

            // call cleanup if resource definition is empty
            if (rscDfn.getResourceCount() == 0)
            {
                controller.cleanup();
            }

            reportSuccess(rscData.getUuid());
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

        return interComSerializer
                .builder(API_LST_RSC, msgId)
                .resourceList(rscs)
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

            Node node = controller.nodesMap.get(nodeName);

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
                controller.getErrorReporter().reportError(
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
            rscData.delete(currentAccCtx.get());
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
}
