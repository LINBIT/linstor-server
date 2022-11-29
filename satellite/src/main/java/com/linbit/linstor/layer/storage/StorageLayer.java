package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.layer.storage.utils.SEDUtils;
import com.linbit.linstor.layer.storage.utils.SharedStorageUtils;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Singleton
public class StorageLayer implements DeviceLayer
{
    private final AccessContext storDriverAccCtx;
    private final DeviceProviderMapper deviceProviderMapper;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final StltSecurityObjects secObjs;

    @Inject
    public StorageLayer(
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        DeviceProviderMapper deviceProviderMapperRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorProviderRef,
        StltSecurityObjects secObjsRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        deviceProviderMapper = deviceProviderMapperRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorProviderRef;
        secObjs = secObjsRef;
    }

    @Override
    public void initialize()
    {
        for (DeviceProvider devProvider : deviceProviderMapper.getDriverList())
        {
            devProvider.initialize();
        }
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(Props localNodeProps)
        throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();
        for (DeviceProvider devProvider : deviceProviderMapper.getDriverList())
        {
            LocalPropsChangePojo pojo = devProvider.setLocalNodeProps(localNodeProps);
            if (pojo != null)
            {
                // TODO we could implement a safeguard here such that a layer can only change/delete properties
                // from its own namespace.

                ret.putAll(pojo);
            }
        }

        return ret;
    }

    @Override
    public boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException
    {
        StateFlags<Flags> rscFlags = layerDataRef.getAbsResource().getStateFlags();
        if (rscFlags.isSet(storDriverAccCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(layerDataRef);
        }
        else
        {
            boolean isActive = rscFlags.isSet(storDriverAccCtx, Resource.Flags.INACTIVE);
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                layerDataRef,
                new ResourceState(
                    isActive,
                    // no (drbd) connections to peers
                    Collections.emptyMap(),
                    null, // will be mapped to unknown
                    isActive,
                    null,
                    null
                )
            );
        }
        return true;
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
    public void prepare(
        Set<AbsRscLayerObject<Resource>> rscObjList,
        Set<AbsRscLayerObject<Snapshot>> snapObjList
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Map<DeviceProvider, Pair<List<VlmProviderObject<Resource>>, List<VlmProviderObject<Snapshot>>>> groupedData;
        groupedData = new HashMap<>();

        for (AbsRscLayerObject<Resource> rscLayerObject : rscObjList)
        {
            for (VlmProviderObject<Resource> vlmProviderObject : rscLayerObject.getVlmLayerObjects().values())
            {
                getOrCreatePair(
                    groupedData,
                    getDevProviderByVlmObj(vlmProviderObject)
                ).objA.add(vlmProviderObject);
            }
        }

        for (AbsRscLayerObject<Snapshot> snapLayerObject : snapObjList)
        {
            for (VlmProviderObject<Snapshot> snapVlmProviderObject : snapLayerObject.getVlmLayerObjects().values())
            {
                getOrCreatePair(
                    groupedData,
                    getDevProviderByVlmObj(snapVlmProviderObject)
                ).objB.add(snapVlmProviderObject);
            }
        }
        for (Entry<DeviceProvider, Pair<List<VlmProviderObject<Resource>>, List<VlmProviderObject<Snapshot>>>> entry :
            groupedData.entrySet())
        {
            DeviceProvider deviceProvider = entry.getKey();
            Pair<List<VlmProviderObject<Resource>>, List<VlmProviderObject<Snapshot>>> pair = entry.getValue();

            deviceProvider.prepare(pair.objA, pair.objB);
        }
    }

    private Pair<List<VlmProviderObject<Resource>>, List<VlmProviderObject<Snapshot>>> getOrCreatePair(
        Map<DeviceProvider, Pair<List<VlmProviderObject<Resource>>, List<VlmProviderObject<Snapshot>>>> groupedData,
        DeviceProvider deviceProvider
    )
    {
        Pair<List<VlmProviderObject<Resource>>, List<VlmProviderObject<Snapshot>>> pair = groupedData
            .get(deviceProvider);
        if (pair == null)
        {
            pair = new Pair<>(new ArrayList<>(), new ArrayList<>());
            groupedData.put(deviceProvider, pair);
        }
        return pair;
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmObj)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        getDevProviderByVlmObj(vlmObj).updateGrossSize(vlmObj);
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmObj)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        // just copy (for now) usableSize = allocateSize and let the DeviceProviders recalculate the allocatedSize
        vlmObj.setUsableSize(vlmObj.getAllocatedSize());
        getDevProviderByVlmObj(vlmObj).updateGrossSize(vlmObj);
    }

    @Override
    public void process(
        AbsRscLayerObject<Resource> rscLayerData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        Map<DeviceProvider, List<VlmProviderObject<Resource>>> groupedVolumes =
            rscLayerData == null ? // == null when processing unprocessed snapshots
                Collections.emptyMap() :
                rscLayerData.streamVlmLayerObjects().collect(Collectors.groupingBy(this::getDevProviderByVlmObj));

        Map<DeviceProvider, List<VlmProviderObject<Snapshot>>> groupedSnapshotVolumes = snapshotList.stream()
            .flatMap(
                snap -> LayerRscUtils.getRscDataByProvider(
                    AccessUtils.execPrivileged(() -> snap.getLayerData(storDriverAccCtx)), DeviceLayerKind.STORAGE
                ).stream()
            )
            .flatMap(snapData -> snapData.getVlmLayerObjects().values().stream())
            .collect(Collectors.groupingBy(this::getDevProviderByVlmObj));

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
            List<VlmProviderObject<Resource>> vlmDataList = groupedVolumes.get(devProvider);
            List<VlmProviderObject<Snapshot>> snapVlmList = groupedSnapshotVolumes.get(devProvider);

            if (vlmDataList == null)
            {
                vlmDataList = Collections.emptyList();
            }
            if (snapVlmList == null)
            {
                snapVlmList = Collections.emptyList();
            }

            /*
             * Issue:
             * We might be in a path where we should take a snapshot of a DRBD resource.
             * If this DRBD resource has external meta-data, we have the following problem:
             * We are on one of two scenarios (might be more if combined with other layers):
             * 1) we are in the ""-path (data)
             * 2) we are in the ".meta"-path (meta-data)
             * In whichever case we are, we also have the order to take a snapshot.
             * The current snapVlmList will contain both, "" and ".meta" snapLayerData for both
             * cases.
             * That means that in the first case we only give the DeviceProvider the rscData of
             * "", but the order to create snapshot of "" AND ".meta". The second case obviously
             * also has a similar issue.
             *
             * As a first approach we filter here those snapLayerData which have a corresponding
             * rscLayerData. This solves the above mentioned issue
             *
             * However, this alone is not good enough, because i.e. deleting a snapshot
             * does not require any rscLayerData, which means the filtering from the mentioned
             * approach will filter all snapLayerData which prevents us from deleting
             * snapshots ever again.
             *
             * Therefore we add an exception. If the list of rscLayerData is empty, we do not
             * filter anything, as those operations *should* only cause resource-independent
             * operations, which are fine for all "*"-paths
             */

            snapVlmList = filterSnapVlms(vlmDataList, snapVlmList);

            devProvider.process(vlmDataList, snapVlmList, apiCallRc);

            for (VlmProviderObject<Resource> vlmData : vlmDataList)
            {
                if (
                    vlmData.exists() &&
                    !SharedStorageUtils.isNeededBySharedResource(storDriverAccCtx, vlmData) &&
                    ((Volume) vlmData.getVolume()).getFlags().isSet(storDriverAccCtx, Volume.Flags.DELETE)
                )
                {
                    throw new ImplementationError(
                        devProvider.getClass().getSimpleName() + " did not delete the volume " + vlmData
                    );
                }
            }
        }
    }

    private List<VlmProviderObject<Snapshot>> filterSnapVlms(
        List<VlmProviderObject<Resource>> vlmDataListRef,
        List<VlmProviderObject<Snapshot>> snapVlmListRef
    )
    {
        List<VlmProviderObject<Snapshot>> ret;
        if (vlmDataListRef.isEmpty())
        {
            // no filter
            ret = snapVlmListRef;
        }
        else
        {
            Set<String> suffixedRscNameSet = new HashSet<>();
            for (VlmProviderObject<Resource> vlmData : vlmDataListRef)
            {
                suffixedRscNameSet.add(vlmData.getRscLayerObject().getSuffixedResourceName());
            }
            ret = new ArrayList<>();
            for (VlmProviderObject<Snapshot> snapVlmData : snapVlmListRef)
            {
                if (suffixedRscNameSet.contains(snapVlmData.getRscLayerObject().getSuffixedResourceName()))
                {
                    ret.add(snapVlmData);
                }
            }
        }
        return ret;
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

    private DeviceProvider getDevProviderByVlmObj(VlmProviderObject<?> vlmLayerObject)
    {
        return deviceProviderMapper.getDeviceProviderByKind(vlmLayerObject.getProviderKind());
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

    @Override
    public SpaceInfo getStoragePoolSpaceInfo(StorPool storPool)
        throws AccessDeniedException, StorageException
    {
        return deviceProviderMapper.getDeviceProviderByStorPool(storPool).getSpaceInfo(storPool);
    }

    @Override
    public LocalPropsChangePojo checkStorPool(StorPool storPool, boolean update)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        LocalPropsChangePojo setLocalNodePojo;

        DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderByStorPool(storPool);
        setLocalNodePojo = deviceProvider.setLocalNodeProps(storPool.getNode().getProps(storDriverAccCtx));
        if (update)
        {
            deviceProvider.update(storPool);
        }

        // check for locked SEDs
        Optional<Props> sedNSProps = storPool.getProps(storDriverAccCtx).getNamespace(ApiConsts.NAMESPC_SED);
        if (sedNSProps.isPresent())
        {
            byte[] masterKey = secObjs.getCryptKey();
            if (masterKey == null)
            {
                Map<String, String> sedMap = SEDUtils.drivePasswordMap(sedNSProps.get().cloneMap());
                for (String device : sedMap.keySet())
                {
                    try
                    {
                        // check if SEDs is really locked, blkid on the device
                        LsBlkUtils.blkid(extCmdFactory.create(), device);
                    }
                    catch (StorageException stoExc)
                    {
                        throw new StorageException("SED drive '" + device + "' locked. Need master-passphrase!");
                    }
                }
            }
        }
        LocalPropsChangePojo checkCfgPropsPojo = deviceProvider.checkConfig(storPool);

        LocalPropsChangePojo ret = null;
        if (setLocalNodePojo != null || checkCfgPropsPojo != null)
        {
            ret = new LocalPropsChangePojo();
            ret.putAll(setLocalNodePojo);
            ret.putAll(checkCfgPropsPojo);
        }

        return ret;
    }
}
