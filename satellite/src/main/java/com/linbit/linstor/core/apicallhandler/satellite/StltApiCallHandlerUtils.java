package com.linbit.linstor.core.apicallhandler.satellite;

import static java.util.stream.Collectors.toList;

import com.linbit.ImplementationError;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.DeviceProviderMapper;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.provider.DeviceProvider;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.locks.LockGuard;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;
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

    @Inject
    public StltApiCallHandlerUtils(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        StorageLayer storageLayerRef,
        DeviceProviderMapper deviceProviderMapperRef
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
    }

    public Map<Volume.Key, Either<Long, ApiRcException>> getVlmAllocatedCapacities(
        Set<StorPoolName> storPoolFilter,
        Set<ResourceName> resourceFilter
    )
    {
        Map<Volume.Key, Either<Long, ApiRcException>> allocatedMap = new HashMap<>();

        try (LockGuard ignored = LockGuard.createLocked(
            nodesMapLock.readLock(), rscDfnMapLock.readLock(), storPoolDfnMapLock.readLock()))
        {
            for (StorPool storPool : controllerPeerConnector.getLocalNode().streamStorPools(apiCtx).collect(toList()))
            {
                if (storPool.getDriverKind().usesThinProvisioning() &&
                    (storPoolFilter.isEmpty() || storPoolFilter.contains(storPool.getName())))
                {
                    for (Volume vlm : storPool.getVolumes(apiCtx))
                    {
                        if (resourceFilter.isEmpty() || resourceFilter.contains(vlm.getResourceDefinition().getName()))
                        {
                            allocatedMap.put(vlm.getKey(), getVlmAllocatedOrError(vlm));
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
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
                if (storPool.getDriverKind().usesThinProvisioning() == thin)
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

    private Either<Long, ApiRcException> getVlmAllocatedOrError(Volume vlm)
        throws AccessDeniedException
    {
        return Either.left(getVlmAllocated(vlm));
    }

    private Long getVlmAllocated(Volume vlm)
        throws AccessDeniedException
    {
        long allocated;
        StorPool storPool = vlm.getStorPool(apiCtx);
        DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderByStorPool(storPool);
        if (deviceProvider == null)
        {
            throw new ApiRcException(ApiCallRcImpl
                .entryBuilder(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Device provider for pool '" + storPool.getName() + "' not found"
                )
                .build()
            );
        }
        else
        {
            allocated = ProviderUtils.getAllocatedSize(vlm, apiCtx);
        }
        return allocated;
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
        return result;
    }

    public SpaceInfo getStoragePoolSpaceInfo(StorPool storPool)
        throws AccessDeniedException, StorageException
    {
        storageLayer.checkStorPool(storPool);
        return new SpaceInfo(
            storageLayer.getCapacity(storPool),
            storageLayer.getFreeSpace(storPool)
        );
    }
}
