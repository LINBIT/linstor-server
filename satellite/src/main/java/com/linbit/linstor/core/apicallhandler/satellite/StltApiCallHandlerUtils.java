package com.linbit.linstor.core.apicallhandler.satellite;

import static java.util.stream.Collectors.toList;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.DeviceProviderMapper;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

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
    private LockGuardFactory lockGuardFactory;

    @Inject
    public StltApiCallHandlerUtils(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        StorageLayer storageLayerRef,
        DeviceProviderMapper deviceProviderMapperRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        nodesMapLock = nodesMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storageLayer = storageLayerRef;
        deviceProviderMapper = deviceProviderMapperRef;
        lockGuardFactory = lockGuardFactoryRef;
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
                    DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderByStorPool(storPool);
                    if (deviceProvider == null)
                    {
                        ApiRcException apiRcException = new ApiRcException(ApiCallRcImpl
                            .entryBuilder(
                                ApiConsts.FAIL_UNKNOWN_ERROR,
                                "Device provider for pool '" + storPool.getName() + "' not found"
                            )
                            .build()
                        );

                        for (VlmProviderObject vlmProviderObject : storPool.getVolumes(apiCtx))
                        {
                            Volume vlm = vlmProviderObject.getVolume();
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
                List<VlmProviderObject> vlmDataList = new ArrayList<>();

                for (StorPool storPool : entry.getValue())
                {
                    for (VlmProviderObject vlmProviderObject : storPool.getVolumes(apiCtx))
                    {
                        Volume vlm = vlmProviderObject.getVolume();
                        if (resourceFilter.isEmpty() || resourceFilter.contains(vlm.getResourceDefinition().getName()))
                        {
                            List<RscLayerObject> rscLayerData = LayerUtils.getChildLayerDataByKind(
                                vlm.getResource().getLayerData(apiCtx),
                                DeviceLayerKind.STORAGE
                            );
                            for (RscLayerObject rlo : rscLayerData)
                            {
                                vlmDataList.add(rlo.getVlmProviderObject(vlm.getVolumeDefinition().getVolumeNumber()));
                            }
                        }
                    }
                }

                try
                {
                    deviceProvider.prepare(vlmDataList, Collections.emptyList());
                    for (VlmProviderObject vlmProviderObject : vlmDataList)
                    {
                        try
                        {
                            deviceProvider.updateAllocatedSize(vlmProviderObject);
                            allocatedMap.put(
                                vlmProviderObject.getVolume().getKey(),
                                Either.left(vlmProviderObject.getAllocatedSize())
                            );
                        }
                        catch (StorageException exc)
                        {
                            ApiRcException apiRcException = new ApiRcException(ApiCallRcImpl
                                .entryBuilder(
                                    ApiConsts.FAIL_UNKNOWN_ERROR,
                                    "Device provider threw a storage exception"
                                )
                                .setCause(exc.getCauseText())
                                .setDetails(exc.getDetailsText())
                                .build()
                            );
                            allocatedMap.put(
                                vlmProviderObject.getVolume().getKey(),
                                Either.right(apiRcException)
                            );
                        }
                    }
                }
                catch (StorageException exc)
                {
                    ApiRcException apiRcException = new ApiRcException(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_UNKNOWN_ERROR,
                            "Device provider threw a storage exception"
                        )
                        .setCause(exc.getCauseText())
                        .setDetails(exc.getDetailsText())
                        .build()
                    );
                    for (VlmProviderObject vlmProviderObject : vlmDataList)
                    {
                        allocatedMap.put(
                            vlmProviderObject.getVolume().getKey(),
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

    public Map<StorPool, Either<SpaceInfo, ApiRcException>> getAllSpaceInfo(boolean thin)
    {
        Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceMap = new HashMap<>();

        Lock nodesMapReadLock = nodesMapLock.readLock();
        Lock storPoolDfnMapReadLock = storPoolDfnMapLock.readLock();

        try
        {
            nodesMapReadLock.lock();
            storPoolDfnMapReadLock.lock();

            for (StorPool storPool : controllerPeerConnector.getLocalNode().streamStorPools(apiCtx).collect(toList()))
            {
                if (storPool.getDeviceProviderKind().usesThinProvisioning() == thin)
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

    private Either<SpaceInfo, ApiRcException> getStoragePoolSpaceInfoOrError(StorPool storPool)
        throws AccessDeniedException
    {
        Either<SpaceInfo, ApiRcException> result;
        try
        {
            result = Either.left(getStoragePoolSpaceInfo(storPool));
        }
        catch (StorageException storageExc)
        {
            result = Either.right(new ApiRcException(ApiCallRcImpl
                .entryBuilder(ApiConsts.FAIL_UNKNOWN_ERROR, "Failed to query free space from storage pool")
                .setCause(storageExc.getMessage())
                .build(),
                storageExc
            ));
        }
        catch (DatabaseException exc)
        {
            throw new ImplementationError(exc);
        }
        return result;
    }

    public SpaceInfo getStoragePoolSpaceInfo(StorPool storPool)
        throws AccessDeniedException, StorageException, DatabaseException
    {
        storageLayer.checkStorPool(storPool);
        return new SpaceInfo(
            storageLayer.getCapacity(storPool),
            storageLayer.getFreeSpace(storPool)
        );
    }
}
