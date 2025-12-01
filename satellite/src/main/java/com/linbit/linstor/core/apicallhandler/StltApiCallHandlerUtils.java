package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.ExternalFileMap;
import com.linbit.linstor.core.CoreModule.KeyValueStoreMap;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.CoreModule.RemoteMap;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMapExtName;
import com.linbit.linstor.core.CoreModule.ResourceGroupMap;
import com.linbit.linstor.core.CoreModule.ScheduleMap;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltExternalFileHandler;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyStorPool;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

public class StltApiCallHandlerUtils
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;

    private final DeviceProviderMapper deviceProviderMapper;
    private final Provider<DeviceManager> devMgr;
    private final ExternalFileMap extFilesMap;
    private final StltExternalFileHandler stltExtFileHandler;
    private final KeyValueStoreMap kvsMap;
    private final NodesMap nodesMap;
    private final RemoteMap remoteMap;
    private final ResourceDefinitionMap rscDfnMap;
    private final ResourceDefinitionMapExtName rscDfnExtNameMap;
    private final ResourceGroupMap rscGrpMap;
    private final ScheduleMap scheduleMap;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final CloneService cloneService;

    @Inject
    public StltApiCallHandlerUtils(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CoreModule.ExternalFileMap extFilesMapRef,
        StltExternalFileHandler stltExtFileHandlerRef,
        CoreModule.KeyValueStoreMap kvsMapRef,
        CoreModule.NodesMap nodesMapRef,
        CoreModule.RemoteMap remoteMapRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CoreModule.ResourceDefinitionMapExtName rscDfnExtNameMapRef,
        CoreModule.ResourceGroupMap rscGrpMapRef,
        CoreModule.ScheduleMap scheduleMapRef,
        CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        DeviceProviderMapper deviceProviderMapperRef,
        CloneService cloneServiceRef,
        Provider<DeviceManager> devMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        extFilesMap = extFilesMapRef;
        stltExtFileHandler = stltExtFileHandlerRef;
        kvsMap = kvsMapRef;
        nodesMap = nodesMapRef;
        remoteMap = remoteMapRef;
        rscDfnMap = rscDfnMapRef;
        rscDfnExtNameMap = rscDfnExtNameMapRef;
        rscGrpMap = rscGrpMapRef;
        scheduleMap = scheduleMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
        deviceProviderMapper = deviceProviderMapperRef;
        cloneService = cloneServiceRef;
        devMgr = devMgrProviderRef;
    }

    public Map<Volume.Key, Either<Long, ApiRcException>> getVlmAllocatedCapacities(
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        Map<Volume.Key, Either<Long, ApiRcException>> allocatedMap = new HashMap<>();

        StltReadOnlyInfo stltReadOnlyInfo = devMgr.get().getReadOnlyData();
        // this method deliberately does not use the StorPoolInfo interface, since the getReadOnlyVolumes method is not
        // part of that interface, since that would require StorPool also to implement it. For now this is not
        // necessary, since this method should not be called / working with the actual StorPool and Volumes, but only
        // with the read-only versions
        Map<DeviceProvider, List<StltReadOnlyInfo.ReadOnlyStorPool>> storPoolsPerDeviceProvider = new HashMap<>();
        for (ReadOnlyStorPool storPoolInfo : stltReadOnlyInfo.getStorPoolReadOnlyInfoList())
        {
            if (storPoolInfo.getDeviceProviderKind().usesThinProvisioning() &&
                (storPoolFilter.isEmpty() || storPoolFilter.contains(storPoolInfo.getName())))
            {
                DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderBy(storPoolInfo);

                List<StltReadOnlyInfo.ReadOnlyStorPool> list = storPoolsPerDeviceProvider.get(deviceProvider);
                if (list == null)
                {
                    list = new ArrayList<>();
                    storPoolsPerDeviceProvider.put(deviceProvider, list);
                }
                list.add(storPoolInfo);
            }
        }

        for (Entry<DeviceProvider, List<StltReadOnlyInfo.ReadOnlyStorPool>> entry : storPoolsPerDeviceProvider
            .entrySet())
        {
            DeviceProvider deviceProvider = entry.getKey();

            List<ReadOnlyVlmProviderInfo> vlmDataList = new ArrayList<>();

            for (StltReadOnlyInfo.ReadOnlyStorPool roStorPool : entry.getValue())
            {
                for (ReadOnlyVlmProviderInfo roVlm : roStorPool.getReadOnlyVolumes())
                {
                    if (resourceFilter.isEmpty() || resourceFilter.contains(roVlm.getResourceName()))
                    {
                        vlmDataList.add(roVlm);
                    }
                }
            }

            try
            {
                // we must not call deviceProvider.prepare(...), since that WILL interfere with the device-manager run.
                // The prepare method populates provider-internal caches based on "lvs" or "zfs list". Recalculating
                // those cached values and overriding based on our read-only-query while the device-manager is still
                // relying on those cached values being based on the actual volumes that are currently being processed
                // is a _very_ bad idea.

                Map<ReadOnlyVlmProviderInfo, Long> allocatedSizeResult = deviceProvider.fetchAllocatedSizes(
                    vlmDataList
                );
                for (Entry<ReadOnlyVlmProviderInfo, Long> allocatedSizeEntry : allocatedSizeResult.entrySet())
                {
                    @Nullable Long value = allocatedSizeEntry.getValue();
                    if (value != null)
                    {
                        @Nullable Either<Long, ApiRcException> valueFromOtherRscSuffixes = allocatedMap.get(
                            allocatedSizeEntry.getKey().getVolumeKey()
                        );
                        if (valueFromOtherRscSuffixes != null)
                        {
                            if (valueFromOtherRscSuffixes instanceof Either.Left)
                            {
                                Long oldValue = ((Either.Left<Long, ApiRcException>) valueFromOtherRscSuffixes).get();

                                allocatedMap.put(
                                    allocatedSizeEntry.getKey().getVolumeKey(),
                                    Either.left(oldValue + value)
                                );
                            }
                        }
                        else
                        {
                            allocatedMap.put(
                                allocatedSizeEntry.getKey().getVolumeKey(),
                                Either.left(value)
                            );
                        }
                    }
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError(accDeniedExc);
            }
            catch (StorageException exc)
            {
                String reportNumber = errorReporter.reportError(exc);
                ApiRcException apiRcException = new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "The device provider generated a StorageException. " +
                                "Error report number: " + reportNumber
                        )
                        .setCause(exc.getCauseText())
                        .setDetails(exc.getDetailsText())
                        .build()
                );
                for (ReadOnlyVlmProviderInfo roVlm : vlmDataList)
                {
                    allocatedMap.put(
                        roVlm.getVolumeKey(),
                        Either.right(apiRcException)
                    );
                }
            }
        }

        return allocatedMap;
    }

    public Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> getAllSpaceInfo()
    {
        return getSpaceInfo(ignored -> true);
    }

    public Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> getSpaceInfo(boolean thin)
    {
        return getSpaceInfo(storPoolInfo -> storPoolInfo.getDeviceProviderKind().usesThinProvisioning() == thin);
    }

    private Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> getSpaceInfo(
        Predicate<StorPoolInfo> shouldIncludeSpTestRef
    )
    {
        Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> spaceMap = new HashMap<>();

        StltReadOnlyInfo roInfo = devMgr.get().getReadOnlyData();

        for (StorPoolInfo storPoolInfo : roInfo.getStorPoolReadOnlyInfoList())
        {
            if (shouldIncludeSpTestRef.test(storPoolInfo))
            {
                spaceMap.put(storPoolInfo, getStoragePoolSpaceInfoOrError(storPoolInfo));
            }
        }

        return spaceMap;
    }

    private Either<SpaceInfo, ApiRcException> getStoragePoolSpaceInfoOrError(StorPoolInfo storPoolInfo)
    {
        Either<SpaceInfo, ApiRcException> result;
        try
        {
            var spaceInfo = getStoragePoolSpaceInfo(storPoolInfo, false);
            errorReporter.logInfo("SpaceInfo: %s -> %d/%d",
                storPoolInfo.getName(),
                spaceInfo.freeCapacity,
                spaceInfo.totalCapacity
            );
            result = Either.left(spaceInfo);
        }
        catch (StorageException storageExc)
        {
            var apiRcExc = new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR, "Failed to query free space from storage pool")
                .setCause(storageExc.getMessage())
                .build(),
                storageExc
            );
            errorReporter.reportError(apiRcExc);
            result = Either.right(apiRcExc);
        }
        return result;
    }

    public SpaceInfo getStoragePoolSpaceInfo(StorPoolInfo storPoolInfo, boolean update)
        throws StorageException
    {
        return devMgr.get().getSpaceInfo(storPoolInfo, update);
    }

    public void updateStorPoolMinIoSizes(
        final ControllerPeerConnector ctrlPeerConnector,
        final CtrlStltSerializer interComSerializer
    )
        throws AccessDeniedException
    {
        final LocalPropsChangePojo propsChange = new LocalPropsChangePojo();
        final @Nullable Node localNode = ctrlPeerConnector.getLocalNode();
        if (localNode != null)
        {
            for (StorPoolDefinition storPoolDfn : storPoolDfnMap.values())
            {
                final @Nullable StorPool storPoolObj = localNode.getStorPool(apiCtx, storPoolDfn.getName());
                if (storPoolObj != null)
                {
                    final DeviceProvider devProvider = deviceProviderMapper.getDeviceProviderBy(storPoolObj);
                    final DeviceProviderKind devProviderKind = devProvider.getDeviceProviderKind();
                    if (devProviderKind == DeviceProviderKind.LVM ||
                        devProviderKind == DeviceProviderKind.LVM_THIN ||
                        devProviderKind == DeviceProviderKind.ZFS ||
                        devProviderKind == DeviceProviderKind.ZFS_THIN)
                    {
                        try
                        {
                            AbsStorageProvider<?, ?, ?> storProvider = (AbsStorageProvider<?, ?, ?>) devProvider;
                            storProvider.updateMinIoSize(storPoolObj, propsChange);
                        }
                        catch (ClassCastException ignored)
                        {
                            // Not a storage provider
                        }
                    }
                }
            } // end for loop

            final @Nullable Peer controllerPeer = ctrlPeerConnector.getControllerPeer();
            if (controllerPeer != null)
            {
                controllerPeer.sendMessage(
                    interComSerializer
                        .onewayBuilder(InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT)
                        .updateLocalProps(propsChange)
                        .build(),
                    InternalApiConsts.API_UPDATE_LOCAL_PROPS_FROM_STLT
                );
            }
        }
    }

    /**
     * This method assumes reconfiguration writelock is taken
     */
    public void clearCoreMaps()
    {
        extFilesMap.clear();
        stltExtFileHandler.clear(); // also clear internal cache
        kvsMap.clear();
        nodesMap.clear();
        remoteMap.clear();
        rscDfnMap.clear();
        rscDfnExtNameMap.clear();
        rscGrpMap.clear();
        scheduleMap.clear();
        storPoolDfnMap.clear();
        cloneService.clear();
    }

    public void clearCaches()
    {
        devMgr.get().clearReadOnlyStltInfo(); // avoid working with outdated data
        LvmUtils.recacheNext();
    }
}
