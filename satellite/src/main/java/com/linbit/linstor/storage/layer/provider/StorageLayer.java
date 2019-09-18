package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.Volume.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.DeviceProviderMapper;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Singleton
public class StorageLayer implements DeviceLayer
{
    private final AccessContext storDriverAccCtx;
    private final DeviceProviderMapper deviceProviderMapper;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;

    @Inject
    public StorageLayer(
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        DeviceProviderMapper deviceProviderMapperRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorProviderRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        deviceProviderMapper = deviceProviderMapperRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorProviderRef;
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
    public void resourceFinished(RscLayerObject layerDataRef) throws AccessDeniedException
    {
        if (layerDataRef.getResource().getStateFlags().isSet(storDriverAccCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(layerDataRef);
        }
        else
        {
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                layerDataRef,
                new UsageState(
                    true,
                    null, // will be mapped to unknown
                    true
                )
            );
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

    public Set<StorPool> getChangedStorPools()
    {
        Set<StorPool> changedStorPools = new TreeSet<>();
        for (DeviceProvider deviceProvider : deviceProviderMapper.getDriverList())
        {
            changedStorPools.addAll(deviceProvider.getChangedStorPools());
        }
        return changedStorPools;
    }

    @Override
    public void prepare(Set<RscLayerObject> rscObjList, Set<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Map<DeviceProvider, Pair<List<VlmProviderObject>, List<SnapshotVolume>>> groupedData;
        groupedData = new HashMap<>();

        for (RscLayerObject rscLayerObject : rscObjList)
        {
            for (VlmProviderObject vlmProviderObject : rscLayerObject.getVlmLayerObjects().values())
            {
                getOrCreatePair(
                    groupedData,
                    getDevProviderByVlmObj(vlmProviderObject)
                ).objA.add(vlmProviderObject);
            }
        }

        for (Snapshot snapshot : snapshots)
        {
            for (SnapshotVolume snapVlm : snapshot.getAllSnapshotVolumes(storDriverAccCtx))
            {
                Map<String, DeviceProvider> classifier = getDeviceProviders(snapVlm);
                for (DeviceProvider devProvider : classifier.values())
                {
                    getOrCreatePair(
                        groupedData,
                        devProvider
                    ).objB.add(snapVlm);
                }
            }
        }
        for (Entry<DeviceProvider, Pair<List<VlmProviderObject>, List<SnapshotVolume>>> entry : groupedData.entrySet())
        {
            DeviceProvider deviceProvider = entry.getKey();
            Pair<List<VlmProviderObject>, List<SnapshotVolume>> pair = entry.getValue();

            deviceProvider.prepare(pair.objA, pair.objB);
        }
    }

    private Pair<List<VlmProviderObject>, List<SnapshotVolume>> getOrCreatePair(
        Map<DeviceProvider, Pair<List<VlmProviderObject>, List<SnapshotVolume>>> groupedData,
        DeviceProvider deviceProvider
    )
    {
        Pair<List<VlmProviderObject>, List<SnapshotVolume>> pair = groupedData.get(deviceProvider);
        if (pair == null)
        {
            pair = new Pair<>(new ArrayList<>(), new ArrayList<>());
            groupedData.put(deviceProvider, pair);
        }
        return pair;
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmObj) throws AccessDeniedException, DatabaseException
    {
        getDevProviderByVlmObj(vlmObj).updateGrossSize(vlmObj);
    }

    @Override
    public void process(RscLayerObject rscLayerData, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        Map<DeviceProvider, List<VlmProviderObject>> groupedVolumes =
            rscLayerData == null ? // == null when processing unprocessed snapshots
                Collections.emptyMap() :
                rscLayerData.streamVlmLayerObjects().collect(Collectors.groupingBy(this::getDevProviderByVlmObj));

        Map<DeviceProvider, List<SnapshotVolume>> groupedSnapshotVolumes = new HashMap<>();
        for (Snapshot snapshot : snapshots)
        {
            for (SnapshotVolume snapshotVolume : snapshot.getAllSnapshotVolumes(storDriverAccCtx))
            {
                Map<String, DeviceProvider> deviceProviders = getDeviceProviders(snapshotVolume);
                for (DeviceProvider devProvider : deviceProviders.values())
                {
                    List<SnapshotVolume> list = groupedSnapshotVolumes.get(devProvider);
                    if (list == null)
                    {
                        list = new ArrayList<>();
                        groupedSnapshotVolumes.put(devProvider, list);
                    }
                    list.add(snapshotVolume);
                }
            }
        }

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
                if (vlmData.exists() && vlmData.getVolume().getFlags().isSet(storDriverAccCtx, Volume.Flags.DELETE))
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

    private Map<String, DeviceProvider> getDeviceProviders(SnapshotVolume snapVlm)
    {
        Map<String, DeviceProvider> devProviderMap = new TreeMap<>();
        try
        {
            StorPool storPool = snapVlm.getStorPool(storDriverAccCtx);
            devProviderMap.put(
                "",
                deviceProviderMapper.getDeviceProviderByStorPool(storPool)
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProviderMap;
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
