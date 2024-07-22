package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo;
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
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.layer.storage.utils.SEDUtils;
import com.linbit.linstor.layer.storage.utils.SharedStorageUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.Either;
import com.linbit.utils.Pair;

import javax.annotation.Nullable;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Singleton
public class StorageLayer implements DeviceLayer
{
    private static final String SUSPEND_IO_NOT_SUPPORTED_ERR_MSG =
        "Suspending / Resuming IO for Storage resources is not supported";

    private final AccessContext storDriverAccCtx;
    private final DeviceProviderMapper deviceProviderMapper;
    private final ExtCmdFactory extCmdFactory;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final StltSecurityObjects secObjs;
    private final ErrorReporter errorReporter;

    @Inject
    public StorageLayer(
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        DeviceProviderMapper deviceProviderMapperRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorProviderRef,
        StltSecurityObjects secObjsRef,
        ErrorReporter errorReporterRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;
        deviceProviderMapper = deviceProviderMapperRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorProviderRef;
        secObjs = secObjsRef;
        errorReporter = errorReporterRef;
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
    public boolean isSuspendIoSupported()
    {
        return false;
    }

    @Override
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        throw new StorageException(SUSPEND_IO_NOT_SUPPORTED_ERR_MSG);
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        throw new StorageException(SUSPEND_IO_NOT_SUPPORTED_ERR_MSG);
    }

    @Override
    public void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef)
        throws DatabaseException, ExtCmdFailedException, StorageException
    {
        throw new StorageException(SUSPEND_IO_NOT_SUPPORTED_ERR_MSG);
    }

    @Override
    public void processResource(
        AbsRscLayerObject<Resource> rscLayerData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        Map<DeviceProvider, List<VlmProviderObject<Resource>>> groupedVolumes =
            rscLayerData == null ? // == null when processing unprocessed snapshots
                Collections.emptyMap() :
                rscLayerData.streamVlmLayerObjects().collect(Collectors.groupingBy(this::getDevProviderByVlmObj));



        Set<DeviceProvider> deviceProviders = new HashSet<>();
        deviceProviders.addAll(
            groupedVolumes.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet())
        );


        for (DeviceProvider devProvider : deviceProviders)
        {
            List<VlmProviderObject<Resource>> vlmDataList = groupedVolumes.get(devProvider);

            if (vlmDataList == null)
            {
                vlmDataList = Collections.emptyList();
            }

            devProvider.processVolumes(vlmDataList, apiCallRc);

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

    @Override
    public boolean processSnapshot(AbsRscLayerObject<Snapshot> snapLayerDataRef, ApiCallRcImpl apiCallRcRef)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException,
        AbortLayerProcessingException
    {
        Map<DeviceProvider, List<VlmProviderObject<Snapshot>>> groupedSnapshotVolumes = new HashMap<>();
        for (VlmProviderObject<Snapshot> storSnapVlmData : snapLayerDataRef.getVlmLayerObjects().values())
        {
            groupedSnapshotVolumes.computeIfAbsent(
                getDevProviderByVlmObj(storSnapVlmData),
                ignored -> new ArrayList<>()
            )
                .add(storSnapVlmData);
        }

        Set<DeviceProvider> deviceProviders = new HashSet<>();
        deviceProviders.addAll(
            groupedSnapshotVolumes.entrySet()
                .stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> entry.getKey())
                .collect(Collectors.toSet())
        );
        for (DeviceProvider devProvider : deviceProviders)
        {
            List<VlmProviderObject<Snapshot>> snapVlmList = groupedSnapshotVolumes.get(devProvider);
            if (snapVlmList == null)
            {
                snapVlmList = Collections.emptyList();
            }


            devProvider.processSnapshotVolumes(snapVlmList, apiCallRcRef);
        }
        return true;
    }

    public Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> getFreeSpaceOfAccessedStoagePools()
        throws AccessDeniedException
    {
        Map<StorPoolInfo, Either<SpaceInfo, ApiRcException>> spaceMap = new HashMap<>();
        Set<StorPool> changedStorPools = new HashSet<>();
        for (DeviceProvider deviceProvider : deviceProviderMapper.getDriverList())
        {
            changedStorPools.addAll(deviceProvider.getChangedStorPools());
        }

        @Nullable StltReadOnlyInfo.ReadOnlyNode roNode = null;
        for (StorPool storPool : changedStorPools)
        {
            if (roNode == null)
            {
                roNode = StltReadOnlyInfo.ReadOnlyNode.copyFrom(storPool.getNode(), storDriverAccCtx);
            }
            else if (!roNode.getUuid().equals(storPool.getNode().getUuid()))
            {
                throw new ImplementationError("Satellite modified storage pool from more than one nodes");
            }
            spaceMap.put(
                StltReadOnlyInfo.ReadOnlyStorPool.copyFrom(roNode, storPool, storDriverAccCtx),
                getStoragePoolSpaceInfoOrError(storPool)
            );
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
    public SpaceInfo getStoragePoolSpaceInfo(StorPoolInfo storPoolInfo)
        throws AccessDeniedException, StorageException
    {
        return deviceProviderMapper.getDeviceProviderBy(storPoolInfo).getSpaceInfo(storPoolInfo);
    }

    @Override
    public LocalPropsChangePojo checkStorPool(StorPoolInfo storPoolInfo, boolean update)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        @Nullable LocalPropsChangePojo setLocalNodePojo;

        DeviceProvider deviceProvider = deviceProviderMapper.getDeviceProviderBy(storPoolInfo);
        setLocalNodePojo = deviceProvider.setLocalNodeProps(
            storPoolInfo.getReadOnlyNode().getReadOnlyProps(storDriverAccCtx)
        );
        @Nullable LocalPropsChangePojo updatePropsPojo = null;
        if (update)
        {
            if (storPoolInfo instanceof StorPool)
            {
                updatePropsPojo = deviceProvider.update((StorPool) storPoolInfo);
            }
            else
            {
                throw new ImplementationError("Cannot update a read only storPool instance!");
            }
        }

        // check for locked SEDs
        @Nullable ReadOnlyProps sedNSProps = storPoolInfo.getReadOnlyProps(storDriverAccCtx)
            .getNamespace(ApiConsts.NAMESPC_SED);
        if (sedNSProps != null)
        {
            byte[] masterKey = secObjs.getCryptKey();
            if (masterKey == null)
            {
                Map<String, String> sedMap = SEDUtils.drivePasswordMap(sedNSProps.cloneMap());
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
        return LocalPropsChangePojo.merge(
            setLocalNodePojo,
            deviceProvider.checkConfig(storPoolInfo),
            updatePropsPojo
        );
    }
}
