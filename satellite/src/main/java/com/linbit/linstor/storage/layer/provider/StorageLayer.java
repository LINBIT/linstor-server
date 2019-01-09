package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
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
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
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
public class StorageLayer implements DeviceLayer
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
    public void prepare(Set<RscLayerObject> rscObjList, Set<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<VlmProviderObject>> groupedVolumes = rscObjList.stream()
            .flatMap(RscLayerObject::streamVlmLayerObjects)
            .collect(Collectors.groupingBy(this::getDevProviderByVlmObj));

        Map<DeviceProvider, List<SnapshotVolume>> groupedSnapshotVolumes = snapshots.stream()
            .flatMap(snapshot ->
                AccessUtils.execPrivileged(() -> snapshot.getAllSnapshotVolumes(storDriverAccCtx).stream())
            )
            .collect(Collectors.groupingBy(this::classifier));

        for (Entry<DeviceProvider, List<VlmProviderObject>> entry : groupedVolumes.entrySet())
        {
            DeviceProvider deviceProvider = entry.getKey();
            List<VlmProviderObject> vlmDataList = entry.getValue();

            if (!vlmDataList.isEmpty())
            {
                // TODO: RAID: maybe we also need something like snapshotVolumeLayerObject
                List<SnapshotVolume> snapVlms = groupedSnapshotVolumes.get(deviceProvider);
                if (snapVlms == null)
                {
                    snapVlms = Collections.emptyList();
                }

                deviceProvider.prepare(vlmDataList, snapVlms);
            }

            // FIXME: this should only be done once on fullSync and whenever a storpool changes
            Set<StorPool> affectedStorPools = vlmDataList.stream()
                .map(vlmObj -> AccessUtils.execPrivileged(() -> vlmObj.getVolume().getStorPool(storDriverAccCtx)))
                .collect(Collectors.toSet());
            for (StorPool storPool : affectedStorPools)
            {
                deviceProvider.checkConfig(storPool);
            }
        }
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmObj) throws AccessDeniedException, SQLException
    {
        getDevProviderByVlmObj(vlmObj).updateGrossSize(vlmObj);
    }

    @Override
    public void process(RscLayerObject rscLayerData, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        Map<DeviceProvider, List<VlmProviderObject>> groupedVolumes =
            rscLayerData == null ? // == null when processing unprocessed snapshots
                Collections.emptyMap() :
                rscLayerData.streamVlmLayerObjects().collect(Collectors.groupingBy(this::getDevProviderByVlmObj));

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
            List<VlmProviderObject> vlmDataList = groupedVolumes.get(devProvider);
            List<SnapshotVolume> snapVlmList = groupedSnapshotVolumes.get(devProvider);

            if (vlmDataList == null)
            {
                vlmDataList = Collections.emptyList();
            }
            if (snapVlmList == null)
            {
                snapVlmList = Collections.emptyList();
            }
            devProvider.process(vlmDataList, snapVlmList, apiCallRc);

            for (VlmProviderObject vlmData : vlmDataList)
            {
                if (vlmData.exists() && vlmData.getVolume().getFlags().isSet(storDriverAccCtx, VlmFlags.DELETE))
                {
                    throw new ImplementationError(
                        devProvider.getClass().getSimpleName() + " did not delete the volume " + vlmData
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

    private DeviceProvider getDevProviderByVlmObj(VlmProviderObject vlmLayerObject)
    {
        return deviceProviderMapper.getDeviceProviderByKind(vlmLayerObject.getProviderKind());
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
}
