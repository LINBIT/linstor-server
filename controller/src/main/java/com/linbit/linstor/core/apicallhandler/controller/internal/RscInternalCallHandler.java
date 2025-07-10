package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.pojo.CapacityInfoPojo;
import com.linbit.linstor.core.apicallhandler.CtrlRscLayerDataMerger;
import com.linbit.linstor.core.apicallhandler.CtrlSnapLayerDataMerger;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.ebs.EbsStatusManagerService;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.provider.utils.ProviderUtils;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.tasks.RetryResourcesTask;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import javax.inject.Inject;
import javax.inject.Provider;

import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RscInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;
    private final CtrlApiDataLoader apiDataLoader;

    private final NodeRepository nodeRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final LockGuardFactory lockGuardFactory;

    private final CtrlRscLayerDataMerger layerRscDataMerger;
    private final CtrlSnapLayerDataMerger layerSnapDataMerger;
    private final RetryResourcesTask retryResourceTask;
    private final CtrlSatelliteUpdater stltUpdater;
    private final SnapshotShippingInternalApiCallHandler snapShipIntHandler;
    private final EbsStatusManagerService ebsStatusMgr;

    @Inject
    public RscInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        NodeRepository nodeRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscLayerDataMerger layerRscDataMergerRef,
        CtrlSnapLayerDataMerger layerSnapDataMergerRef,
        RetryResourcesTask retryResourceTaskRef,
        CtrlApiDataLoader ctrlApiDataLoader,
        CtrlSatelliteUpdater stltUpdaterRef,
        SnapshotShippingInternalApiCallHandler snapShipIntHandlerRef,
        EbsStatusManagerService ebsStatusMgrRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        nodeRepository = nodeRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        lockGuardFactory = lockGuardFactoryRef;
        layerRscDataMerger = layerRscDataMergerRef;
        layerSnapDataMerger = layerSnapDataMergerRef;
        retryResourceTask = retryResourceTaskRef;
        apiDataLoader = ctrlApiDataLoader;
        stltUpdater = stltUpdaterRef;
        snapShipIntHandler = snapShipIntHandlerRef;
        ebsStatusMgr = ebsStatusMgrRef;
    }

    public void handleResourceRequest(
        String nodeNameStr,
        String rscNameStr
    )
    {
        try (
            LockGuard lg = lockGuardFactory.create()
                .read(LockObj.NODES_MAP, LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP)
                .postLinstorLocks(peer.get().getSerializerLock().readLock())
                .build()
        )
        {
            NodeName nodeName = new NodeName(nodeNameStr);

            Node node = nodeRepository.get(apiCtx, nodeName); // TODO use CtrlApiLoader.loadNode

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
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_APPLY_RSC)
                            .resource(rsc, fullSyncTimestamp, updateId)
                            .build()
                    );
                }
                else
                {
                    peer.get().sendMessage(
                        ctrlStltSerializer
                            .onewayBuilder(InternalApiConsts.API_APPLY_RSC_DELETED)
                            .deletedResource(rscNameStr, fullSyncTimestamp, updateId)
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

    public void updateVolume(
        String resourceName,
        RscLayerDataApi rscLayerDataPojoRef,
        Map<String, String> rscPropsRef,
        Map<Integer, Map<String, String>> vlmPropsRef,
        Map<String, Map<String, String>> snapPropsRef,
        Map<String, Map<Integer, Map<String, String>>> snapVlmPropsRef,
        Map<String, RscLayerDataApi> snapLayersRef,
        List<CapacityInfoPojo> capacityInfos
    )
    {
        try (
            LockGuard lg = lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP)
                .build()
        )
        {
            /*
             * be careful setting this to true - otherwise we could run into infinite loop between
             * ctrl -> rsc changed -> stlt
             * ...
             * ctrl <- rsc applied <- stlt
             * ctrl -> rsc changed -> stlt
             *
             */
            boolean updateSatellite = false;

            NodeName nodeName = peer.get().getNode().getName();
            Map<StorPoolName, CapacityInfoPojo> storPoolToCapacityInfoMap = capacityInfos.stream().collect(
                Collectors.toMap(
                    freeSpacePojo -> LinstorParsingUtils.asStorPoolName(freeSpacePojo.getStorPoolName()),
                    Function.identity()
                )
            );
            ResourceDefinition rscDfn = resourceDefinitionRepository.get(apiCtx, new ResourceName(resourceName));
            Resource rsc = rscDfn.getResource(apiCtx, nodeName);

            if (rsc.getCreateTimestamp().isPresent() &&
                rsc.getCreateTimestamp().get().equals(new Date(AbsResource.CREATE_DATE_INIT_VALUE)))
            {
                rsc.setCreateTimestamp(apiCtx, new Date(Instant.now().toEpochMilli()));
            }

            layerRscDataMerger.mergeLayerData(rsc, rscLayerDataPojoRef, false);
            mergeStltProps(rscPropsRef, rsc.getProps(apiCtx));

            // also merge snap and snapVlm props
            for (SnapshotDefinition snapDfn : rscDfn.getSnapshotDfns(apiCtx))
            {
                Snapshot snapshot = snapDfn.getSnapshot(apiCtx, nodeName);
                if (snapshot != null)
                {
                    // no need to merge snap.rsc.props. satellite should only have modified snap.props.
                    mergeStltProps(snapPropsRef.get(snapDfn.getName().value), snapshot.getSnapProps(apiCtx));

                    RscLayerDataApi snapLayerDataPojo = snapLayersRef.get(snapDfn.getName().value);
                    if (snapLayerDataPojo != null)
                    {
                        layerSnapDataMerger.mergeLayerData(snapshot, snapLayerDataPojo, false);
                    }

                    Map<Integer, Map<String, String>> allSnapVlmProps = snapVlmPropsRef.get(snapDfn.getName().value);
                    if (allSnapVlmProps != null)
                    {
                        for (SnapshotVolumeDefinition snapVlmDfn : snapDfn.getAllSnapshotVolumeDefinitions(apiCtx))
                        {
                            VolumeNumber snapVlmNr = snapVlmDfn.getVolumeNumber();
                            SnapshotVolume snapVlm = snapshot.getVolume(snapVlmNr);
                            Map<String, String> snapVlmPropPojo = allSnapVlmProps.get(snapVlmNr.value);
                            if (snapVlm != null && snapVlmPropPojo != null)
                            {

                                Props snapVlmProps = snapVlm.getSnapVlmProps(apiCtx);

                                // check has to be done before merging, but adding has to be done after merge is
                                // complete
                                boolean addToEbsStatusMgr = EbsUtils.isEbs(apiCtx, snapshot) &&
                                    EbsUtils.getEbsSnapId(snapVlmProps, RscLayerSuffixes.SUFFIX_DATA) == null &&
                                    EbsUtils.getEbsSnapId(snapVlmPropPojo, RscLayerSuffixes.SUFFIX_DATA) != null;
                                mergeStltProps(snapVlmPropPojo, snapVlmProps);
                                if (addToEbsStatusMgr)
                                {
                                    // pojo has prop which is not (yet) stored / merged.
                                    // -> register this snapshot
                                    ebsStatusMgr.addIfEbs(snapshot);
                                }
                            }
                        }
                    }
                }
            }

            Set<AbsRscLayerObject<Resource>> storageResources = LayerRscUtils.getRscDataByLayer(
                rsc.getLayerData(apiCtx),
                DeviceLayerKind.STORAGE
            );

            Iterator<Volume> iterateVolumes = rsc.iterateVolumes();
            while (iterateVolumes.hasNext())
            {
                Volume vlm = iterateVolumes.next();
                VolumeNumber vlmNr = vlm.getVolumeDefinition().getVolumeNumber();

                VlmLayerDataApi vlmLayerDataPojo = rscLayerDataPojoRef.getVolumeMap().get(vlmNr.value);

                Map<String, String> vlmPropPojo = vlmPropsRef.get(vlmNr.value);
                Props vlmProps = vlm.getProps(apiCtx);

                // check before merge before add
                boolean addVlmToEbsStatusMgr = EbsUtils.isEbs(apiCtx, rsc) &&
                    !EbsUtils.hasAnyEbsProp(vlmProps) &&
                    EbsUtils.hasAnyEbsProp(vlmPropPojo);
                mergeStltProps(vlmPropPojo, vlmProps);
                if (addVlmToEbsStatusMgr)
                {
                    // pojo has prop which is not (yet) stored / merged.
                    // -> register this resource
                    ebsStatusMgr.addIfEbs(rsc);
                }

                if (vlmLayerDataPojo != null)
                {
                    vlm.setDevicePath(apiCtx, vlmLayerDataPojo.getDevicePath());
                    vlm.setUsableSize(apiCtx, vlmLayerDataPojo.getUsableSize());
                    vlm.setAllocatedSize(apiCtx, ProviderUtils.getAllocatedSize(vlm, apiCtx));
                    vlm.clearReports();

                    for (AbsRscLayerObject<Resource> storageRsc : storageResources)
                    {
                        VlmProviderObject<Resource> vlmProviderObject = storageRsc.getVlmProviderObject(vlmNr);
                        if (vlmProviderObject != null)
                        {
                            StorPool storPool = vlmProviderObject.getStorPool();

                            CapacityInfoPojo capacityInfo =
                                storPoolToCapacityInfoMap.get(storPool.getName());

                            storPool.getFreeSpaceTracker().vlmCreationFinished(
                                apiCtx,
                                vlmProviderObject,
                                capacityInfo == null ? null : capacityInfo.getFreeCapacity(),
                                    capacityInfo == null ? null : capacityInfo.getTotalCapacity()
                                );

                            if (capacityInfo == null && !storPool.getDeviceProviderKind().usesThinProvisioning())
                            {
                                errorReporter.logWarning(
                                    String.format(
                                        "No freespace info for storage pool '%s' on node: %s",
                                        storPool.getName().value,
                                        nodeName.displayValue
                                    )
                                );
                            }
                        }
                    }
                }
                else
                {
                    errorReporter.logWarning(
                        String.format(
                            "Tried to update a volume with missing layer data. Node: %s, Resource: %s, VolumeNr: %d",
                            nodeName.displayValue,
                            rscDfn.getName().displayValue,
                            vlm.getVolumeDefinition().getVolumeNumber().value
                        )
                    );
                }
            }

            /*
             * TODO: instead of this loop, we should introduce a "notifySnapshotApplied"
             * and put the logic of this loop there
             */
            for (SnapshotDefinition snapDfn : rsc.getResourceDefinition().getSnapshotDfns(apiCtx))
            {
                Snapshot snap = snapDfn.getSnapshot(apiCtx, nodeName);
                if (
                    snap != null && snap.getFlags().isSet(apiCtx, Snapshot.Flags.SHIPPING_TARGET) &&
                    snapShipIntHandler.startShipping(snap))
                {
                    updateSatellite = true;
                }
            }

            StateFlags<Flags> rscFlags = rsc.getStateFlags();

            if (rscFlags.isSet(apiCtx, Resource.Flags.RESTORE_FROM_SNAPSHOT))
            {
                rscFlags.disableFlags(apiCtx, Resource.Flags.RESTORE_FROM_SNAPSHOT);
                rsc.getResourceDefinition().getProps(apiCtx)
                    .removeProp(InternalApiConsts.KEY_BACKUP_NODE_IDS_TO_RESET, ApiConsts.NAMESPC_BACKUP_SHIPPING);
                updateSatellite = true;
            }

            retryResourceTask.remove(rsc);
            ctrlTransactionHelper.commit();

            // only update satellite after transaction was successfully committed
            if (updateSatellite)
            {
                stltUpdater.updateSatellites(rsc);
            }
        }
        catch (InvalidNameException | AccessDeniedException | DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public void handleResourceFailed(String nodeName, String rscName, Map<String, RscLayerDataApi> snapLayersRef)
    {
        try (
            LockGuard lg = lockGuardFactory.create()
                .read(LockObj.NODES_MAP)
                .write(LockObj.RSC_DFN_MAP, LockObj.STOR_POOL_DFN_MAP)
                .build()
        )
        {
            Resource rsc = apiDataLoader.loadRsc(
                nodeName,
                rscName,
                true
            );
            for (SnapshotDefinition snapDfn : rsc.getResourceDefinition().getSnapshotDfns(apiCtx))
            {
                Snapshot snapshot = snapDfn.getSnapshot(apiCtx, new NodeName(nodeName));
                if (snapshot != null)
                {
                    RscLayerDataApi snapLayerDataPojo = snapLayersRef.get(snapDfn.getName().value);
                    if (snapLayerDataPojo != null)
                    {
                        layerSnapDataMerger.mergeLayerData(snapshot, snapLayerDataPojo, false);
                    }
                }
            }
            retryResourceTask.add(rsc, null);
        }
        catch (InvalidNameException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void mergeStltProps(@Nullable Map<String, String> srcPropsMap, Props targetProps)
    {
        if (srcPropsMap != null)
        {
            /*
             * only merge properties from the "Satellite" namespace.
             * other properties might have been added or deleted in the meantime, but the satellite
             * did not get the update yet so those properties would be undone / restored now.
             */
            @Nullable Props stltNs = targetProps.getNamespace(ApiConsts.NAMESPC_STLT);
            if (stltNs != null)
            {
                stltNs.keySet().retainAll(srcPropsMap.keySet());
            }
            try
            {
                for (Entry<String, String> entry : srcPropsMap.entrySet())
                {
                    String key = entry.getKey();
                    if (key.startsWith(ApiConsts.NAMESPC_STLT))
                    {
                        targetProps.setProp(key, entry.getValue());
                    }
                }
            }
            catch (InvalidKeyException | InvalidValueException | AccessDeniedException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
    }
}
