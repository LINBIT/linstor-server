package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.DeviceProviderMapper;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.utils.ProviderUtils;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@Singleton
public class StorageLayer implements ResourceLayer
{
    private final AccessContext storDriverAccCtx;
    private final DeviceProviderMapper deviceProviderMapper;
    private final ExtCmdFactory extCmdFactory;


    @Inject
    public StorageLayer(
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        DeviceProviderMapper deviceProviderMapperRef,
        ExtCmdFactory extCmdFactoryRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        deviceProviderMapper = deviceProviderMapperRef;
        extCmdFactory = extCmdFactoryRef;
    }

    @Override
    public void setLocalNodeProps(Props localNodeProps)
    {
        for (DeviceProvider devProvider : deviceProviderMapper.getDriverList())
        {
            devProvider.setLocalNodeProps(localNodeProps);
        }
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void clearCache() throws StorageException
    {
        for (DeviceProvider deviceProvider : deviceProviderMapper.getDriverList())
        {
            deviceProvider.clearCache();
        }
    }

    @Override
    public void prepare(List<Resource> resources, List<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<Volume>> groupedVolumes = resources.stream()
            .flatMap(Resource::streamVolumes)
            .collect(Collectors.groupingBy(this::classifier));

        Map<DeviceProvider, List<SnapshotVolume>> groupedSnapshotVolumes = snapshots.stream()
            .flatMap(snapshot ->
                AccessUtils.execPrivileged(() -> snapshot.getAllSnapshotVolumes(storDriverAccCtx).stream())
            )
            .collect(Collectors.groupingBy(this::classifier));

        for (Entry<DeviceProvider, List<Volume>> entry : groupedVolumes.entrySet())
        {
            DeviceProvider deviceProvider = entry.getKey();
            List<Volume> volumes = entry.getValue();
            if (!volumes.isEmpty())
            {
                List<SnapshotVolume> snapVlms = groupedSnapshotVolumes.get(deviceProvider);
                if (snapVlms == null)
                {
                    snapVlms = Collections.emptyList();
                }
                deviceProvider.prepare(volumes, snapVlms);
            }

            // FIXME: this should only be done once on fullSync and whenever a storpool changes
            Set<StorPool> affectedStorPools = volumes.stream()
                .map(vlm -> AccessUtils.execPrivileged(() -> vlm.getStorPool(storDriverAccCtx)))
                .collect(Collectors.toSet());
            for (StorPool storPool : affectedStorPools)
            {
                deviceProvider.checkConfig(storPool);
            }
        }
    }

    @Override
    public void updateGrossSize(Volume childVlm, Volume parentVolume) throws AccessDeniedException, SQLException
    {
        childVlm.setUsableSize(storDriverAccCtx, parentVolume.getAllocatedSize(storDriverAccCtx));
        // the childvlm.setAllocateSize method is called from the corresponding DeviceProvider
    }

    @Override
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<Volume>> groupedVolumes =
            rsc == null ?
                Collections.emptyMap() :
                    rsc.streamVolumes().collect(Collectors.groupingBy(this::classifier));

        Map<DeviceProvider, List<SnapshotVolume>> groupedSnapshotVolumes = snapshots.stream()
            .flatMap(snapshot ->
                AccessUtils.execPrivileged(() -> snapshot.getAllSnapshotVolumes(storDriverAccCtx).stream())
            )
            .collect(Collectors.groupingBy(this::classifier));

        Set<DeviceProvider> deviceProviders = new HashSet<>();
        deviceProviders.addAll(
            groupedVolumes.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet())
        );
        deviceProviders.addAll(
            groupedSnapshotVolumes.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet())
        );

        for (DeviceProvider devProvider : deviceProviders)
        {
            List<Volume> vlmList = groupedVolumes.get(devProvider);
            List<SnapshotVolume> snapVlmList = groupedSnapshotVolumes.get(devProvider);

            if (vlmList == null)
            {
                vlmList = Collections.emptyList();
            }
            if (snapVlmList == null)
            {
                snapVlmList = Collections.emptyList();
            }
            devProvider.process(vlmList, snapVlmList, apiCallRc);

            for (Volume vlm : vlmList)
            {
                if (!vlm.isDeleted() && vlm.getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    throw new ImplementationError(
                        devProvider.getClass().getSimpleName() + " did not delete the volume " + vlm
                    );
                }
            }
        }
    }

    public long getFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return deviceProviderMapper.getDeviceProviderByStorPool(storPool).getPoolFreeSpace(storPool);
    }

    public long getCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return deviceProviderMapper.getDeviceProviderByStorPool(storPool).getPoolCapacity(storPool);
    }

    public Map<StorPool, Either<SpaceInfo, ApiRcException>> getFreeSpaceOfAccessedStoagePools()
        throws AccessDeniedException
    {
        Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceMap = new HashMap<>();
        Set<StorPool> changedStorPools = new HashSet<>();
        for (DeviceProvider deviceProvider : deviceProviderMapper.getDriverList())
        {
            changedStorPools.addAll(deviceProvider.getChangedStorPools());
        }
        for (StorPool storPool : changedStorPools)
        {
            spaceMap.put(storPool, getStoragePoolSpaceInfoOrError(storPool));
        }
        return spaceMap;
    }

    private DeviceProvider classifier(Volume vlm)
    {
        DeviceProvider devProvider = null;
        try
        {
            devProvider = deviceProviderMapper.getDeviceProviderByStorPool(vlm.getStorPool(storDriverAccCtx));
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProvider;
    }

    private DeviceProvider classifier(SnapshotVolume snapVlm)
    {
        DeviceProvider devProvider = null;
        try
        {
            devProvider = deviceProviderMapper.getDeviceProviderByStorPool(snapVlm.getStorPool(storDriverAccCtx));
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProvider;
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
        return new SpaceInfo(
            getCapacity(storPool),
            getFreeSpace(storPool)
        );
    }

    public void checkStorPool(StorPool storPool) throws StorageException, AccessDeniedException
    {
        DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderByStorPool(storPool);
        deviceProvider.setLocalNodeProps(storPool.getNode().getProps(storDriverAccCtx));
        deviceProvider.checkConfig(storPool);
    }

    public long getAllocatedSize(Volume vlm, AccessContext accCtx)
        throws StorageException, AccessDeniedException
    {
        return ProviderUtils.getAllocatedSize(vlm, extCmdFactory.create(), accCtx);
    }
}
