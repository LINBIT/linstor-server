package com.linbit.linstor.core.apicallhandler.controller;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeId;
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
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.VlmUpdatePojo;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ConfigModule;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlObjectFactories;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionMgr;

import static com.linbit.linstor.api.ApiConsts.API_LST_RSC;
import static com.linbit.linstor.api.ApiConsts.FAIL_INVLD_STOR_POOL_NAME;
import static com.linbit.linstor.api.ApiConsts.FAIL_NOT_FOUND_DFLT_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.KEY_STOR_POOL_NAME;
import static com.linbit.linstor.api.ApiConsts.MASK_STOR_POOL;
import static com.linbit.linstor.api.ApiConsts.MASK_WARN;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.utils.StringUtils.firstLetterCaps;
import static java.util.stream.Collectors.toList;

@Singleton
public class CtrlRscApiCallHandler extends CtrlRscCrtApiCallHandler
{
    private final CtrlClientSerializer clientComSerializer;
    private final ObjectProtection rscDfnMapProt;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final ObjectProtection nodesMapProt;
    private final CoreModule.NodesMap nodesMap;
    private final String defaultStorPoolName;
    private final VolumeDefinitionDataControllerFactory volumeDefinitionDataFactory;
    private final ResponseConverter responseConverter;

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
        @Named(ControllerCoreModule.SATELLITE_PROPS) Props stltConfRef,
        ResourceDataFactory resourceDataFactoryRef,
        VolumeDataFactory volumeDataFactoryRef,
        VolumeDefinitionDataControllerFactory volumeDefinitionDataFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        WhitelistProps whitelistPropsRef,
        ResponseConverter responseConverterRef
    )
    {
        super(
            errorReporterRef,
            apiCtxRef,
            interComSerializer,
            objectFactories,
            transMgrProviderRef,
            peerAccCtxRef,
            peerRef,
            whitelistPropsRef,
            stltConfRef,
            resourceDataFactoryRef,
            volumeDataFactoryRef
        );
        clientComSerializer = clientComSerializerRef;
        rscDfnMapProt = rscDfnMapProtRef;
        rscDfnMap = rscDfnMapRef;
        nodesMapProt = nodesMapProtRef;
        nodesMap = nodesMapRef;
        defaultStorPoolName = defaultStorPoolNameRef;
        volumeDefinitionDataFactory = volumeDefinitionDataFactoryRef;
        responseConverter = responseConverterRef;
    }

    public ApiCallRc createResource(
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<VlmApi> vlmApiList
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeRscContext(
            peer.get(),
            ApiOperation.makeRegisterOperation(),
            nodeNameStr,
            rscNameStr
        );

        try
        {
            ResourceData rsc = createResource0(
                nodeNameStr,
                rscNameStr,
                flagList,
                rscPropsMap,
                vlmApiList
            ).extractApiCallRc(responses);

            commit();

            if (rsc.getVolumeCount() > 0)
            {
                // only notify satellite if there are volumes to deploy.
                // otherwise a bug occurs when an empty resource is deleted
                // the controller instantly deletes it (without marking for deletion first)
                // but doesn't tell the satellite...
                // the next time a resource with the same name will get a different UUID and
                // will cause a conflict (and thus, an exception) on the satellite
                responseConverter.addWithDetail(responses, context, updateSatellites(rsc));
            }

            responseConverter.addWithOp(responses, context,
                ApiSuccessUtils.defaultRegisteredEntry(rsc.getUuid(), getRscDescriptionInline(rsc)));

            Iterator<Volume> vlmIt = rsc.iterateVolumes();
            while (vlmIt.hasNext())
            {
                Volume vlm = vlmIt.next();
                int vlmNr = vlm.getVolumeDefinition().getVolumeNumber().value;

                ApiCallRcEntry vlmCreatedRcEntry = new ApiCallRcEntry();
                vlmCreatedRcEntry.setMessage(
                    "Volume with number '" + vlmNr + "' on resource '" +
                        vlm.getResourceDefinition().getName().displayValue + "' on node '" +
                        vlm.getResource().getAssignedNode().getName().displayValue +
                        "' successfully registered"
                    );
                vlmCreatedRcEntry.setDetails(
                    "Volume UUID is: " + vlm.getUuid().toString()
                );
                vlmCreatedRcEntry.setReturnCode(ApiConsts.MASK_CRT | ApiConsts.MASK_VLM | ApiConsts.CREATED);
                vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_NODE, nodeNameStr);
                vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
                vlmCreatedRcEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmNr));

                responses.addEntry(vlmCreatedRcEntry);
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void checkStorPoolLoaded(
        final Resource rsc,
        StorPool storPool,
        String storPoolNameStr,
        final VolumeDefinition vlmDfn
    )
    {
        if (storPool == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(FAIL_NOT_FOUND_DFLT_STOR_POOL, "The storage pool '" + storPoolNameStr + "' " +
                    "for resource '" + rsc.getDefinition().getName().displayValue + "' " +
                    "for volume number '" + vlmDfn.getVolumeNumber().value + "' " +
                    "is not deployed on node '" + rsc.getAssignedNode().getName().displayValue + "'.")
                .setDetails("The resource which should be deployed had at least one volume definition " +
                    "(volume number '" + vlmDfn.getVolumeNumber().value + "') which LinStor " +
                    "tried to automatically create. " +
                    "The storage pool's name for this new volume was looked for in " +
                    "its volume definition's properties, its resource's properties, its node's " +
                    "properties and finally in a system wide default storage pool name defined by " +
                    "the LinStor controller.")
                .build(),
                new LinStorException("Dependency not found")
            );
        }
    }

    private void checkBackingDiskWithDiskless(final Resource rsc, final StorPool storPool)
    {
        if (storPool != null && storPool.getDriverKind().hasBackingStorage())
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(FAIL_INVLD_STOR_POOL_NAME, "Storage pool with backing disk not allowed with diskless resource.")
                .setCause(String.format("Resource '%s' flagged as diskless, but a storage pool '%s' " +
                        "with backing disk was specified.",
                    rsc.getDefinition().getName().displayValue,
                    storPool.getName().displayValue))
                .setCorrection("Use a storage pool with a diskless driver or remove the diskless flag.")
                .build(),
                new LinStorException("Incorrect storage pool used.")
            );
        }
    }

    private ApiCallRc warnAndFlagDiskless(Resource rsc, final StorPool storPool)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        if (storPool != null && !storPool.getDriverKind().hasBackingStorage())
        {
            responses.addEntry(ApiCallRcImpl
                .entryBuilder(
                    MASK_WARN | MASK_STOR_POOL,
                    "Resource will be automatically flagged diskless."
                )
                .setCause(String.format("Used storage pool '%s' is diskless, " +
                    "but resource was not flagged diskless", storPool.getName().displayValue))
                .build()
            );
            try
            {
                rsc.getStateFlags().enableFlags(apiCtx, RscFlags.DISKLESS);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (SQLException exc)
            {
                throw new ApiSQLException(exc);
            }
        }

        return responses;
    }

    /**
     * Resolves the correct storage pool and also handles error/warnings in diskless modes.
     *
     * @param rsc
     * @param prioProps
     * @param vlmDfn
     * @return
     * @throws InvalidKeyException
     * @throws AccessDeniedException
     * @throws InvalidValueException
     * @throws SQLException
     */
    private ApiCallRcWith<StorPool> resolveStorPool(
        Resource rsc,
        final PriorityProps prioProps,
        final VolumeDefinition vlmDfn
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        StorPool storPool;
        try
        {
            final boolean isRscDiskless = isDiskless(rsc);
            Props rscProps = getProps(rsc);
            String storPoolNameStr = prioProps.getProp(KEY_STOR_POOL_NAME);
            if (isRscDiskless)
            {
                if (storPoolNameStr == null || "".equals(storPoolNameStr))
                {
                    rscProps.setProp(ApiConsts.KEY_STOR_POOL_NAME, LinStor.DISKLESS_STOR_POOL_NAME);
                    storPool = rsc.getAssignedNode().getDisklessStorPool(apiCtx);
                    storPoolNameStr = LinStor.DISKLESS_STOR_POOL_NAME;
                }
                else
                {
                    storPool = rsc.getAssignedNode().getStorPool(
                        apiCtx,
                        asStorPoolName(storPoolNameStr)
                    );
                }

                checkBackingDiskWithDiskless(rsc, storPool);
            }
            else
            {
                if (storPoolNameStr == null || "".equals(storPoolNameStr))
                {
                    storPoolNameStr = defaultStorPoolName;
                }
                storPool = rsc.getAssignedNode().getStorPool(
                    apiCtx,
                    asStorPoolName(storPoolNameStr)
                );

                responses.addEntries(warnAndFlagDiskless(rsc, storPool));
            }

            checkStorPoolLoaded(rsc, storPool, storPoolNameStr, vlmDfn);
        }
        catch (InvalidKeyException | InvalidValueException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (SQLException exc)
        {
            throw new ApiSQLException(exc);
        }

        return new ApiCallRcWith<>(responses, storPool);
    }

    /**
     * This method really creates the resource and its volumes.
     *
     * This method does NOT:
     * * commit any transaction
     * * update satellites
     * * create success-apiCallRc entries (only error RC in case of exception)
     *
     * @param nodeNameStr
     * @param rscNameStr
     * @param flagList
     * @param rscPropsMap
     * @param vlmApiList
     *
     * @return the newly created resource
     */
    ApiCallRcWith<ResourceData> createResource0(
        String nodeNameStr,
        String rscNameStr,
        List<String> flagList,
        Map<String, String> rscPropsMap,
        List<VlmApi> vlmApiList
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        NodeData node = loadNode(nodeNameStr, true);
        ResourceDefinitionData rscDfn = loadRscDfn(rscNameStr, true);

        NodeId nodeId = getNextFreeNodeId(rscDfn);

        ResourceData rsc = createResource(rscDfn, node, nodeId, flagList);
        Props rscProps = getProps(rsc);

        fillProperties(LinStorObject.RESOURCE, rscPropsMap, rscProps, ApiConsts.FAIL_ACC_DENIED_RSC);

        boolean isRscDiskless = isDiskless(rsc);

        Props rscDfnProps = getProps(rscDfn);
        Props nodeProps = getProps(node);

        Map<Integer, Volume> vlmMap = new TreeMap<>();
        for (VlmApi vlmApi : vlmApiList)
        {
            VolumeDefinitionData vlmDfn = loadVlmDfn(rscDfn, vlmApi.getVlmNr(), true);

            PriorityProps prioProps = new PriorityProps(
                rscProps,
                getProps(vlmDfn),
                rscDfnProps,
                nodeProps
            );

            StorPool storPool;

            String storPoolNameStr;
            storPoolNameStr = vlmApi.getStorPoolName();
            if (storPoolNameStr != null && !storPoolNameStr.isEmpty())
            {
                StorPoolDefinitionData storPoolDfn = loadStorPoolDfn(storPoolNameStr, true);
                storPool = loadStorPool(storPoolDfn, node, true);

                if (isRscDiskless)
                {
                    checkBackingDiskWithDiskless(rsc, storPool);
                }
                else
                {
                    responses.addEntries(warnAndFlagDiskless(rsc, storPool));
                }

                checkStorPoolLoaded(rsc, storPool, storPoolNameStr, vlmDfn);
            }
            else
            {
                storPool = resolveStorPool(rsc, prioProps, vlmDfn).extractApiCallRc(responses);
            }

            VolumeData vlmData = createVolume(rsc, vlmDfn, storPool, vlmApi);

            Props vlmProps = getProps(vlmData);

            fillProperties(LinStorObject.VOLUME, vlmApi.getVlmProps(), vlmProps, ApiConsts.FAIL_ACC_DENIED_VLM);

            vlmMap.put(vlmDfn.getVolumeNumber().value, vlmData);
        }

        Iterator<VolumeDefinition> iterateVolumeDfn = getVlmDfnIterator(rscDfn);
        while (iterateVolumeDfn.hasNext())
        {
            VolumeDefinition vlmDfn = iterateVolumeDfn.next();

            // first check if we probably just deployed a vlm for this vlmDfn
            if (rsc.getVolume(vlmDfn.getVolumeNumber()) == null)
            {
                // not deployed yet.

                PriorityProps prioProps = new PriorityProps(
                    getProps(vlmDfn),
                    rscProps,
                    nodeProps
                );

                StorPool storPool = resolveStorPool(rsc, prioProps, vlmDfn).extractApiCallRc(responses);

                // storPool is guaranteed to be != null
                // create missing vlm with default values
                VolumeData vlm = createVolume(rsc, vlmDfn, storPool, null);
                vlmMap.put(vlmDfn.getVolumeNumber().value, vlm);
            }
        }
        return new ApiCallRcWith<>(responses, rsc);
    }

    private boolean isDiskless(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    public ApiCallRc modifyResource(
        UUID rscUuid,
        String nodeNameStr,
        String rscNameStr,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeRscContext(
            peer.get(),
            ApiOperation.makeModifyOperation(),
            nodeNameStr,
            rscNameStr
        );

        try
        {
            ResourceData rsc = loadRsc(nodeNameStr, rscNameStr, true);

            if (rscUuid != null && !rscUuid.equals(rsc.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UUID_RSC,
                    "UUID-check failed"
                ));
            }

            Props props = getProps(rsc);
            Map<String, String> propsMap = props.map();

            fillProperties(LinStorObject.RESOURCE, overrideProps, props, ApiConsts.FAIL_ACC_DENIED_RSC);

            for (String delKey : deletePropKeys)
            {
                propsMap.remove(delKey);
            }

            commit();

            responseConverter.addWithDetail(responses, context, updateSatellites(rsc));
            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                rsc.getUuid(), getRscDescriptionInline(rsc)));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public ApiCallRc deleteResource(
        String nodeNameStr,
        String rscNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeRscContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr
        );

        try
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, true);

            SatelliteState stltState = rscData.getAssignedNode().getPeer(apiCtx).getSatelliteState();
            SatelliteResourceState rscState = stltState.getResourceStates().get(rscData.getDefinition().getName());

            if (rscState != null && rscState.isInUse() != null && rscState.isInUse())
            {
                responseConverter.addWithOp(responses, context, ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_IN_USE,
                        String.format("Resource '%s' is still in use.", rscNameStr)
                    )
                    .setCause("Resource is mounted/in use.")
                    .setCorrection(String.format("Un-mount resource '%s' on the node '%s'.", rscNameStr, nodeNameStr))
                    .build()
                );
            }
            else
            {
                int volumeCount = rscData.getVolumeCount();
                String successMessage;
                String details;
                String descriptionFirstLetterCaps = firstLetterCaps(getRscDescription(nodeNameStr, rscNameStr));
                if (volumeCount > 0)
                {
                    successMessage = descriptionFirstLetterCaps + " marked for deletion.";
                    details = descriptionFirstLetterCaps + " UUID is: " + rscData.getUuid();
                    markDeleted(rscData);
                }
                else
                {
                    successMessage = descriptionFirstLetterCaps + " deleted.";
                    details = descriptionFirstLetterCaps + " UUID was: " + rscData.getUuid();
                    delete(rscData);
                }

                commit();

                if (volumeCount > 0)
                {
                    // notify satellites
                    responseConverter.addWithDetail(responses, context, updateSatellites(rscData));
                }

                responseConverter.addWithOp(responses, context,
                    ApiCallRcImpl.entryBuilder(ApiConsts.DELETED, successMessage).setDetails(details).build());
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }



    /**
     * This is called if a satellite has deleted its resource to notify the controller
     * that it can delete the resource.
     *
     * @param nodeNameStr Node name where the resource was deleted.
     * @param rscNameStr Resource name of the deleted resource.
     * @return Apicall response for the call.er
     */
    public ApiCallRc resourceDeleted(
        String nodeNameStr,
        String rscNameStr
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeRscContext(
            peer.get(),
            ApiOperation.makeDeleteOperation(),
            nodeNameStr,
            rscNameStr
        );

        try
        {
            ResourceData rscData = loadRsc(nodeNameStr, rscNameStr, false);

            if (rscData == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.WARN_NOT_FOUND,
                    firstLetterCaps(getRscDescription(nodeNameStr, rscNameStr)) + " not found"
                ));
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
            if (rscDfn.getResourceCount() == 0)
            {
                // remove primary flag
                errorReporter.logDebug(
                    String.format("Resource definition '%s' empty, deleting primary flag.", rscNameStr)
                );
                rscDfn.getProps(apiCtx).removeProp(InternalApiConsts.PROP_PRIMARY_SET);

                if (isMarkedForDeletion(rscDfn))
                {
                    deletedRscDfnName = rscDfn.getName();
                    rscDfnUuid = rscDfn.getUuid();
                    delete(rscDfn);
                }
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

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultDeletedEntry(
                rscUuid, getRscDescriptionInline(nodeNameStr, rscNameStr)));

            if (deletedRscDfnName != null)
            {
                responseConverter.addWithOp(
                    responses, context, makeRscDfnDeletedResponse(deletedRscDfnName, rscDfnUuid));
                rscDfnMap.remove(deletedRscDfnName);
            }
            if (deletedNodeName != null)
            {
                nodesMap.remove(deletedNodeName);
                node.getPeer(apiCtx).closeConnection();
                responseConverter.addWithOp(responses, context, makeNodeDeletedResponse(deletedNodeName, nodeUuid));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    byte[] listResources(
        int msgId,
        List<String> filterNodes,
        List<String> filterResources
    )
    {
        ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
        Map<NodeName, SatelliteState> satelliteStates = new HashMap<>();
        try
        {
            rscDfnMapProt.requireAccess(peerAccCtx.get(), AccessType.VIEW);
            nodesMapProt.requireAccess(peerAccCtx.get(), AccessType.VIEW);

            final List<String> upperFilterNodes = filterNodes.stream().map(String::toUpperCase).collect(toList());
            final List<String> upperFilterResources =
                filterResources.stream().map(String::toUpperCase).collect(toList());

            rscDfnMap.values().stream()
                .filter(rscDfn -> upperFilterResources.isEmpty() ||
                    upperFilterResources.contains(rscDfn.getName().value))
                .forEach(rscDfn ->
                {
                    try
                    {
                        for (Resource rsc : rscDfn.streamResource(peerAccCtx.get())
                            .filter(rsc -> upperFilterNodes.isEmpty() ||
                                upperFilterNodes.contains(rsc.getAssignedNode().getName().value))
                            .collect(toList()))
                        {
                            rscs.add(rsc.getApiData(peerAccCtx.get(), null, null));
                            // fullSyncId and updateId null, as they are not going to be serialized anyways
                        }
                    }
                    catch (AccessDeniedException accDeniedExc)
                    {
                        // don't add storpooldfn without access
                    }
                }
                );

            // get resource states of all nodes
            for (final Node node : nodesMap.values())
            {
                if (upperFilterNodes.isEmpty() || upperFilterNodes.contains(node.getName().value))
                {
                    final Peer peer = node.getPeer(peerAccCtx.get());
                    if (peer != null)
                    {
                        Lock readLock = peer.getSatelliteStateLock().readLock();
                        readLock.lock();
                        try
                        {
                            final SatelliteState satelliteState = peer.getSatelliteState();

                            if (satelliteState != null)
                            {
                                final SatelliteState filterStates = new SatelliteState(satelliteState);

                                // states are already complete, we remove all resource that are not interesting from
                                // our clone
                                Set<ResourceName> removeSet = new TreeSet<>();
                                for (ResourceName rscName : filterStates.getResourceStates().keySet())
                                {
                                    if (!(upperFilterResources.isEmpty() ||
                                          upperFilterResources.contains(rscName.value)))
                                    {
                                        removeSet.add(rscName);
                                    }
                                }
                                removeSet.forEach(rscName -> filterStates.getResourceStates().remove(rscName));
                                satelliteStates.put(node.getName(), filterStates);
                            }
                        }
                        finally
                        {
                            readLock.unlock();
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
                .resourceList(rscs, satelliteStates)
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
                Resource rsc = !node.isDeleted() ? node.getResource(apiCtx, rscName) : null;

                long fullSyncTimestamp = peer.get().getFullSyncId();
                long updateId = peer.get().getNextSerializerId();
                // TODO: check if the localResource has the same uuid as rscUuid
                if (rsc != null && !rsc.isDeleted())
                {
                    peer.get().sendMessage(
                        internalComSerializer
                            .builder(InternalApiConsts.API_APPLY_RSC, msgId)
                            .resourceData(rsc, fullSyncTimestamp, updateId)
                            .build()
                    );
                }
                else
                {
                    peer.get().sendMessage(
                        internalComSerializer
                        .builder(InternalApiConsts.API_APPLY_RSC_DELETED, msgId)
                        .deletedResourceData(rscNameStr, fullSyncTimestamp, updateId)
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
                peer.get().closeConnection();
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

    protected final VolumeDefinitionData loadVlmDfn(
        ResourceDefinitionData rscDfn,
        int vlmNr,
        boolean failIfNull
    )
    {
        return loadVlmDfn(rscDfn, asVlmNr(vlmNr), failIfNull);
    }

    protected final VolumeDefinitionData loadVlmDfn(
        ResourceDefinitionData rscDfn,
        VolumeNumber vlmNr,
        boolean failIfNull
    )
    {
        VolumeDefinitionData vlmDfn;
        try
        {
            vlmDfn = volumeDefinitionDataFactory.load(
                peerAccCtx.get(),
                rscDfn,
                vlmNr
            );

            if (failIfNull && vlmDfn == null)
            {
                String rscName = rscDfn.getName().displayValue;
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_NOT_FOUND_VLM_DFN,
                        "Volume definition with number '" + vlmNr.value + "' on resource definition '" +
                            rscName + "' not found."
                    )
                    .setCause("The specified volume definition with number '" + vlmNr.value + "' on resource definition '" +
                        rscName + "' could not be found in the database")
                    .setCorrection("Create a volume definition with number '" + vlmNr.value + "' on resource definition '" +
                        rscName + "' first.")
                    .build()
                );
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "load " + getVlmDfnDescriptionInline(rscDfn.getName().displayValue, vlmNr.value),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return vlmDfn;
    }

    protected final Props getProps(Resource rsc)
    {
        Props props;
        try
        {
            props = rsc.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for resource '" + rsc.getDefinition().getName().displayValue + "' on node '" +
                    rsc.getAssignedNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return props;
    }

    protected final Props getProps(Volume vlm)
    {
        Props props;
        try
        {
            props = vlm.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for volume with number '" + vlm.getVolumeDefinition().getVolumeNumber().value +
                        "' on resource '" + vlm.getResourceDefinition().getName().displayValue + "' " +
                    "on node '" + vlm.getResource().getAssignedNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        return props;
    }

    private void markDeleted(ResourceData rscData)
    {
        try
        {
            rscData.markDeleted(peerAccCtx.get());
            Iterator<Volume> volumesIterator = rscData.iterateVolumes();
            while (volumesIterator.hasNext())
            {
                Volume vlm = volumesIterator.next();
                vlm.markDeleted(peerAccCtx.get());
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getRscDescription(rscData) + " as deleted",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
    }

    private void delete(ResourceData rscData)
    {
        try
        {
            rscData.delete(peerAccCtx.get()); // also deletes all of its volumes
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getRscDescription(rscData),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
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
            throw new ImplementationError(accDeniedExc);
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
            throw new ImplementationError(accDeniedExc);
        }
        return isMarkedForDeletion;
    }

    private ApiCallRc.RcEntry makeRscDfnDeletedResponse(ResourceName rscName, UUID rscDfnUuid)
    {
        String rscDeletedMsg = CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline(rscName.displayValue) +
            " deleted.";
        rscDeletedMsg = rscDeletedMsg.substring(0, 1).toUpperCase() + rscDeletedMsg.substring(1);

        errorReporter.logInfo(rscDeletedMsg);

        return ApiCallRcImpl.entryBuilder(ApiConsts.MASK_RSC_DFN | ApiConsts.DELETED, rscDeletedMsg)
            .putObjRef(ApiConsts.KEY_RSC_DFN, rscName.displayValue)
            .putObjRef(ApiConsts.KEY_UUID, rscDfnUuid.toString())
            .build();
    }

    private ApiCallRc.RcEntry makeNodeDeletedResponse(NodeName nodeName, UUID nodeUuid)
    {
        String rscDeletedMsg = CtrlNodeApiCallHandler.getNodeDescriptionInline(nodeName.displayValue) +
            " deleted.";

        errorReporter.logInfo(rscDeletedMsg);

        return ApiCallRcImpl.entryBuilder(ApiConsts.MASK_NODE | ApiConsts.DELETED, rscDeletedMsg)
            .putObjRef(ApiConsts.KEY_NODE, nodeName.displayValue)
            .putObjRef(ApiConsts.KEY_UUID, nodeUuid.toString())
            .build();
    }

    void updateVolumeData(Peer satellitePeer, String resourceName, List<VlmUpdatePojo> vlmUpdates)
    {
        try
        {
            NodeName nodeName = satellitePeer.getNode().getName();
            ResourceDefinition rscDfn = rscDfnMap.get(new ResourceName(resourceName));
            Resource rsc = rscDfn.getResource(apiCtx, nodeName);

            for (VlmUpdatePojo vlmUpd : vlmUpdates)
            {
                try
                {
                    Volume vlm = rsc.getVolume(new VolumeNumber(vlmUpd.getVolumeNumber()));
                    if (vlm != null)
                    {
                        vlm.setBackingDiskPath(apiCtx, vlmUpd.getBlockDevicePath());
                        vlm.setMetaDiskPath(apiCtx, vlmUpd.getMetaDiskPath());
                    }
                    else
                    {
                        errorReporter.logWarning(
                            String.format(
                                "Tried to update a non existing volume. Node: %s, Resource: %s, VolumeNr: %d",
                                nodeName.displayValue,
                                rscDfn.getName().displayValue,
                                vlmUpd.getVolumeNumber()
                            )
                        );
                    }
                }
                catch (ValueOutOfRangeException ignored)
                {
                }
            }
        }
        catch (InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public static String getRscDescription(Resource resource)
    {
        return getRscDescription(
            resource.getAssignedNode().getName().displayValue, resource.getDefinition().getName().displayValue);
    }

    public static String getRscDescription(String nodeNameStr, String rscNameStr)
    {
        return "Node: " + nodeNameStr + ", Resource: " + rscNameStr;
    }

    public static String getRscDescriptionInline(Resource rsc)
    {
        return getRscDescriptionInline(rsc.getAssignedNode(), rsc.getDefinition());
    }

    public static String getRscDescriptionInline(Node node, ResourceDefinition rscDfn)
    {
        return getRscDescriptionInline(node.getName().displayValue, rscDfn.getName().displayValue);
    }

    public static String getRscDescriptionInline(String nodeNameStr, String rscNameStr)
    {
        return "resource '" + rscNameStr + "' on node '" + nodeNameStr + "'";
    }

    private static ResponseContext makeRscContext(
        Peer peer,
        ApiOperation operation,
        String nodeNameStr,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_NODE, nodeNameStr);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            peer,
            operation,
            getRscDescription(nodeNameStr, rscNameStr),
            getRscDescriptionInline(nodeNameStr, rscNameStr),
            ApiConsts.MASK_RSC,
            objRefs
        );
    }
}
