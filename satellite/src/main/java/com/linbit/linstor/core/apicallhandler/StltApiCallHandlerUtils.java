package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
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
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.storage.DeviceProvider;
import com.linbit.linstor.layer.storage.DeviceProviderMapper;
import com.linbit.linstor.layer.storage.StorageLayer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public class StltApiCallHandlerUtils
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;

    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock storPoolDfnMapLock;
    private final ControllerPeerConnector controllerPeerConnector;
    private final ReadWriteLock rscDfnMapLock;
    private final StorageLayer storageLayer;
    private final DeviceProviderMapper deviceProviderMapper;
    private final LockGuardFactory lockGuardFactory;
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

    @Inject
    public StltApiCallHandlerUtils(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
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
        StorageLayer storageLayerRef,
        DeviceProviderMapper deviceProviderMapperRef,
        LockGuardFactory lockGuardFactoryRef,
        Provider<DeviceManager> devMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
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
        storageLayer = storageLayerRef;
        deviceProviderMapper = deviceProviderMapperRef;
        lockGuardFactory = lockGuardFactoryRef;
        devMgr = devMgrProviderRef;
    }

    public Map<Volume.Key, Either<Long, ApiRcException>> getVlmAllocatedCapacities(
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        Map<Volume.Key, Either<Long, ApiRcException>> allocatedMap = new HashMap<>();

        try (LockGuard ignored = lockGuardFactory.build(
            LockType.READ,
            LockObj.NODES_MAP,
            LockObj.RSC_DFN_MAP,
            LockObj.STOR_POOL_DFN_MAP
            )
        )
        {
            Map<DeviceProvider, List<StorPool>> storPoolsPerDeviceProvider = new HashMap<>();
            for (StorPool storPool : controllerPeerConnector.getLocalNode().streamStorPools(apiCtx).collect(toList()))
            {
                if (storPool.getDeviceProviderKind().usesThinProvisioning() &&
                    (storPoolFilter.isEmpty() || storPoolFilter.contains(storPool.getName())))
                {
                    DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderBy(storPool);
                    if (deviceProvider == null)
                    {
                        ApiRcException apiRcException = new ApiRcException(ApiCallRcImpl
                            .entryBuilder(
                                ApiConsts.FAIL_UNKNOWN_ERROR,
                                "Device provider for pool '" + storPool.getName() + "' not found"
                            )
                            .build()
                        );

                        for (VlmProviderObject<Resource> vlmProviderObject : storPool.getVolumes(apiCtx))
                        {
                            Volume vlm = (Volume) vlmProviderObject.getVolume();
                            if (
                                resourceFilter.isEmpty() ||
                                resourceFilter.contains(vlm.getResourceDefinition().getName())
                            )
                            {
                                allocatedMap.put(vlm.getKey(), Either.right(apiRcException));
                            }
                        }
                    }
                    else
                    {
                        List<StorPool> list = storPoolsPerDeviceProvider.get(deviceProvider);
                        if (list == null)
                        {
                            list = new ArrayList<>();
                            storPoolsPerDeviceProvider.put(deviceProvider, list);
                        }
                        list.add(storPool);

                    }
                }
            }

            for (Entry<DeviceProvider, List<StorPool>> entry : storPoolsPerDeviceProvider.entrySet())
            {
                DeviceProvider deviceProvider = entry.getKey();

                List<VlmProviderObject<Resource>> vlmDataList = new ArrayList<>();

                for (StorPool storPool : entry.getValue())
                {
                    for (VlmProviderObject<Resource> vlmProviderObject : storPool.getVolumes(apiCtx))
                    {
                        Volume vlm = (Volume) vlmProviderObject.getVolume();
                        if (resourceFilter.isEmpty() || resourceFilter.contains(vlm.getResourceDefinition().getName()))
                        {
                            vlmDataList.add(vlmProviderObject);
                        }
                    }
                }

                try
                {
                    deviceProvider.prepare(vlmDataList, Collections.emptyList());
                    for (VlmProviderObject<Resource> vlmProviderObject : vlmDataList)
                    {
                        try
                        {
                            if (!vlmProviderObject.getRscLayerObject().hasIgnoreReason())
                            {
                                deviceProvider.updateAllocatedSize(vlmProviderObject);
                                allocatedMap.put(
                                    ((Volume) vlmProviderObject.getVolume()).getKey(),
                                    Either.left(vlmProviderObject.getAllocatedSize())
                                );
                            }
                        }
                        catch (StorageException exc)
                        {
                            String reportNumber = errorReporter.reportError(exc);
                            ApiRcException apiRcException = new ApiRcException(ApiCallRcImpl
                                .entryBuilder(
                                    ApiConsts.FAIL_UNKNOWN_ERROR,
                                    "The device provider generated a StorageException. " +
                                    "Error report number: " + reportNumber
                                )
                                .setCause(exc.getCauseText())
                                .setDetails(exc.getDetailsText())
                                .build()
                            );
                            allocatedMap.put(
                                ((Volume) vlmProviderObject.getVolume()).getKey(),
                                Either.right(apiRcException)
                            );
                        }
                    }
                }
                catch (StorageException exc)
                {
                    String reportNumber = errorReporter.reportError(exc);
                    ApiRcException apiRcException = new ApiRcException(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "The device provider generated a StorageException. Error report number: " + reportNumber
                        )
                        .setCause(exc.getCauseText())
                        .setDetails(exc.getDetailsText())
                        .build()
                    );
                    for (VlmProviderObject<Resource> vlmProviderObject : vlmDataList)
                    {
                        allocatedMap.put(
                            ((Volume) vlmProviderObject.getVolume()).getKey(),
                            Either.right(apiRcException)
                        );
                    }
                }
            }
        }
        catch (AccessDeniedException | DatabaseException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
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

        Lock nodesMapReadLock = nodesMapLock.readLock();
        Lock storPoolDfnMapReadLock = storPoolDfnMapLock.readLock();

        try
        {
            nodesMapReadLock.lock();
            storPoolDfnMapReadLock.lock();

            for (StorPool storPool : controllerPeerConnector.getLocalNode().streamStorPools(apiCtx).collect(toList()))
            {
                if (shouldIncludeSpTestRef.test(storPool))
                {
                    spaceMap.put(storPool, getStoragePoolSpaceInfoOrError(storPool));
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            errorReporter.reportError(new ImplementationError(exc));
        }
        finally
        {
            storPoolDfnMapReadLock.unlock();
            nodesMapReadLock.unlock();
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
    }
}
