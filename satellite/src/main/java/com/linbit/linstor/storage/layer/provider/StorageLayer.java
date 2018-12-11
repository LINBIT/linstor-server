package com.linbit.linstor.storage.layer.provider;

import com.linbit.ImplementationError;
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
import com.linbit.linstor.storage.DisklessDriverKind;
import com.linbit.linstor.storage.LvmDriverKind;
import com.linbit.linstor.storage.LvmThinDriverKind;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.SwordfishInitiatorDriverKind;
import com.linbit.linstor.storage.SwordfishTargetDriverKind;
import com.linbit.linstor.storage.ZfsDriverKind;
import com.linbit.linstor.storage.ZfsThinDriverKind;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.diskless.DisklessProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmProvider;
import com.linbit.linstor.storage.layer.provider.lvm.LvmThinProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.AbsSwordfishProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishInitiatorProvider;
import com.linbit.linstor.storage.layer.provider.swordfish.SwordfishTargetProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsProvider;
import com.linbit.linstor.storage.layer.provider.zfs.ZfsThinProvider;
import com.linbit.utils.AccessUtils;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Arrays;
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

    private final LvmProvider lvmProvider;
    private final LvmThinProvider lvmThinProvider;
    private final ZfsProvider zfsProvider;
    private final ZfsThinProvider zfsThinProvider;
    private final AbsSwordfishProvider sfTargetProvider;
    private final SwordfishInitiatorProvider sfInitProvider;
    private final DisklessProvider disklessProvider;
    private final List<DeviceProvider> driverList;

    private final Set<StorPool> changedStorPools = new HashSet<>();

    @Inject
    public StorageLayer(
        @DeviceManagerContext AccessContext storDriverAccCtxRef,
        LvmProvider lvmProviderRef,
        LvmThinProvider lvmThinProviderRef,
        ZfsProvider zfsProviderRef,
        ZfsThinProvider zfsThinProviderRef,
        SwordfishTargetProvider sfTargetProviderRef,
        SwordfishInitiatorProvider sfInitProviderRef,
        DisklessProvider disklessProviderRef
    )
    {
        storDriverAccCtx = storDriverAccCtxRef;

        lvmProvider = lvmProviderRef;
        lvmThinProvider = lvmThinProviderRef;
        zfsProvider = zfsProviderRef;
        zfsThinProvider = zfsThinProviderRef;
        sfTargetProvider = sfTargetProviderRef;
        sfInitProvider = sfInitProviderRef;
        disklessProvider = disklessProviderRef;

        driverList = Arrays.asList(
            lvmProvider,
            lvmThinProvider,
            zfsProvider,
            zfsThinProvider,
            sfTargetProvider,
            sfInitProvider,
            disklessProvider
        );
    }

    @Override
    public void setLocalNodeProps(Props localNodeProps)
    {
        for (DeviceProvider devProvider : driverList)
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
        for (DeviceProvider deviceProvider : driverList)
        {
            changedStorPools.addAll(deviceProvider.getAndForgetChangedStorPools());
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
            List<Volume> volumes = entry.getValue();
            if (!volumes.isEmpty())
            {
                DeviceProvider deviceProvider = entry.getKey();
                List<SnapshotVolume> snapVlms = groupedSnapshotVolumes.get(deviceProvider);
                if (snapVlms == null)
                {
                    snapVlms = Collections.emptyList();
                }
                deviceProvider.prepare(volumes, snapVlms);
            }
        }
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
        return classifier(storPool).getPoolFreeSpace(storPool);
    }

    public long getCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        return classifier(storPool).getPoolCapacity(storPool);
    }

    public Map<StorPool, Either<SpaceInfo, ApiRcException>> getFreeSpaceOfAccessedStoagePools()
        throws AccessDeniedException
    {
        Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceMap = new HashMap<>();
        for (StorPool storPool : changedStorPools)
        {
            spaceMap.put(storPool, getStoragePoolSpaceInfoOrError(storPool));
        }
        changedStorPools.clear();
        return spaceMap;
    }

    private DeviceProvider classifier(Volume vlm)
    {
        DeviceProvider devProvider = null;
        try
        {
            devProvider = classifier(vlm.getStorPool(storDriverAccCtx));
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
            devProvider = classifier(snapVlm.getStorPool(storDriverAccCtx));
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return devProvider;
    }

    private DeviceProvider classifier(StorPool storPool)
    {
        StorageDriverKind driverKind = storPool.getDriverKind();

        DeviceProvider devProvider;
        if (driverKind instanceof LvmDriverKind)
        {
            devProvider = lvmProvider;
        }
        else if (driverKind instanceof LvmThinDriverKind)
        {
            devProvider = lvmThinProvider;
        }
        else if (driverKind instanceof ZfsDriverKind)
        {
            devProvider = zfsProvider;
        }
        else if (driverKind instanceof ZfsThinDriverKind)
        {
            devProvider = zfsThinProvider;
        }
        else if (driverKind instanceof SwordfishTargetDriverKind)
        {
            devProvider = sfTargetProvider;
        }
        else if (driverKind instanceof SwordfishInitiatorDriverKind)
        {
            devProvider = sfInitProvider;
        }
        else if (driverKind instanceof DisklessDriverKind)
        {
            devProvider = disklessProvider;
        }
        else
        {
            throw new ImplementationError("Unknown storagerProvider found: " +
                driverKind.getDriverName() + " " + driverKind.getClass().getSimpleName()
            );
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
}
