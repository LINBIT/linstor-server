package com.linbit.linstor.layer.storage.storagespaces;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.utils.WmiHelper;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.StorageSpacesInfo;
import com.linbit.linstor.storage.StorageUtils;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.storagespaces.StorageSpacesData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.StringUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class StorageSpacesProvider extends AbsStorageProvider<StorageSpacesInfo, StorageSpacesData<Resource>, StorageSpacesData<Snapshot>>
{
    private static final int TOLERANCE_FACTOR = 3;

    /**
     * 64 KiB. We are using partitions which should be quite
     * close to the size we request.
     */
    private static final int WORST_CASE_GRANULARITY = 64;
    private boolean rebuildCache;
    private Set<String> dirtyVolumes;

    @Inject
    public StorageSpacesProvider(AbsStorageProviderInit superInitRef)
    {
        super(superInitRef, "STORAGE_SPACES", DeviceProviderKind.STORAGE_SPACES);

        rebuildCache = true;
        dirtyVolumes = new HashSet<>();

        subclassMaintainsInfoListCache = true;
    }

    public StorageSpacesProvider(
        AbsStorageProviderInit superInitRef,
        String typeDescrRef,
        DeviceProviderKind kindRef
    )
    {
        super(superInitRef, typeDescrRef, kindRef);

        rebuildCache = true;
        dirtyVolumes = new HashSet<>();

        subclassMaintainsInfoListCache = true;
    }

    @Override
    protected void updateStates(List<StorageSpacesData<Resource>> vlmDataList, List<StorageSpacesData<Snapshot>> snapVlmDataList)
        throws StorageException, DatabaseException
    {
        List<StorageSpacesData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlmDataList);

        for (StorageSpacesData<?> vlmData : combinedList)
        {
            final StorageSpacesInfo info = infoListCache.get(getFullQualifiedIdentifier(vlmData));
            updateInfo(vlmData, info);

            if (info != null)
            {
                final long expectedSize = vlmData.getExpectedSize();
                final long actualSize = info.getSize();
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.setSizeState(Size.TOO_SMALL);
                    }
                    else
                    {
                        Size sizeState = Size.TOO_LARGE;

                        final long toleratedSize =
                            expectedSize + WORST_CASE_GRANULARITY * TOLERANCE_FACTOR;

                        if (actualSize < toleratedSize)
                        {
                            sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                        }
                        vlmData.setSizeState(sizeState);
                    }
                }
                else
                {
                    vlmData.setSizeState(Size.AS_EXPECTED);
                }
            }
        }
    }

    protected String getFullQualifiedIdentifier(StorageSpacesData<?> vlmDataRef)
    {
        return extractStoragePoolFriendlyName(vlmDataRef) +
            File.separator +
            asIdentifierRaw(vlmDataRef);
    }

    @SuppressWarnings("unchecked")
    protected String asIdentifierRaw(StorageSpacesData<?> vlmData)
    {
        return asLvIdentifier((StorageSpacesData<Resource>) vlmData);
    }

    @SuppressWarnings({ "unchecked" })
    protected void updateInfo(StorageSpacesData<?> vlmDataRef, @Nullable StorageSpacesInfo info)
        throws DatabaseException, StorageException
    {
        if (info == null)
        {
            vlmDataRef.setExists(false);
            vlmDataRef.setStoragePoolFriendlyName(extractStoragePoolFriendlyName(vlmDataRef));
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAllocatedSize(-1);
            vlmDataRef.setIdentifier(asIdentifierRaw(vlmDataRef));
        }
        else
        {
            vlmDataRef.setExists(true);
            vlmDataRef.setStoragePoolFriendlyName(info.getStoragePoolFriendlyName());
            vlmDataRef.setDevicePath(info.getPath());
            vlmDataRef.setIdentifier(info.getIdentifier());
            vlmDataRef.setAllocatedSize(info.getAllocatedSize());
            vlmDataRef.setUsableSize(info.getSize());
        }
    }

    protected String extractStoragePoolFriendlyName(StorageSpacesData<?> vlmData)
    {
        return getStoragePoolFriendlyName(vlmData.getStorPool());
    }

    @Override
    protected void createLvImpl(StorageSpacesData<Resource> vlmData)
        throws StorageException
    {
            /* The partition must be exactly the required size (even if
             * it could be larger), so that DRBD can connect to others.
             */
        long partitionSize = vlmData.getExpectedSize()*1024;
        String provisioningType = "thick";
        if (kind.equals(DeviceProviderKind.STORAGE_SPACES_THIN))
        {
            provisioningType = "thin";
        }

        WmiHelper.run(extCmdFactory, new String[] {
            "virtual-disk",
            "create",
            vlmData.getStoragePoolFriendlyName(),
            vlmData.getIdentifier(),
            String.format("%d", partitionSize),
            provisioningType }
        );

            /* read info from virtual-disk list later */
        dirtyVolumes.add(getFullQualifiedIdentifier(vlmData));
    }

    @Override
    protected void resizeLvImpl(StorageSpacesData<Resource> vlmData)
        throws StorageException
    {
            /* The partition must be exactly the required size (even if
             * it could be larger), so that DRBD can connect to others.
             */
        long partitionSize = vlmData.getExpectedSize()*1024;

        WmiHelper.run(extCmdFactory, new String[] {
            "virtual-disk",
            "resize",
            vlmData.getIdentifier(),
            String.format("%d", partitionSize) }
        );

            /* Re-read info later */
        dirtyVolumes.add(getFullQualifiedIdentifier(vlmData));
    }

    @Override
    protected void deleteLvImpl(StorageSpacesData<Resource> vlmData, String oldLvmId)
        throws StorageException, DatabaseException
    {
        WmiHelper.run(extCmdFactory, new String[] { "virtual-disk", "delete", vlmData.getIdentifier() });

            /* Tell updateInfoListCache() to remove info from cache later */
        dirtyVolumes.add(getFullQualifiedIdentifier(vlmData));
        vlmData.setExists(false);
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> map = new HashMap<>();
        OutputData res;

        res = WmiHelper.run(extCmdFactory, new String[] {
            "storage-pool",
            "list-available"}
        );
        final String[] lines = StringUtils.split(new String(res.stdoutData, StandardCharsets.UTF_8), "\n");
        for (String line: lines)
        {
            final String[] data = StringUtils.split(line.trim(), "\t");

            if (data.length != 3)
            {
                continue;
            }
            Long size;
            Long allocatedSize;
            String storPoolName = data[0];

            try
            {
                size = StorageUtils.parseDecimalAsLong(data[1]);
                allocatedSize = StorageUtils.parseDecimalAsLong(data[2]);
            }
            catch (NumberFormatException exc)
            {
                throw new StorageException("Unable to parse size of Storage pool " + data[0],
                    "Size to parse: " + data[1] + " - " + data[2],
                    null,
                    null,
                    "cmd",
                    exc);
            }
            size = size / 1024;     /* LINSTOR computes in KiB not in bytes */
            allocatedSize = allocatedSize / 1024;

            if (map.putIfAbsent(storPoolName, size - allocatedSize) != null)
            {
                throw new StorageException("Storage Pool " + storPoolName + " exists more than once? Please fix that.",
                    null,
                    null,
                    "cmd",
                    null);
            }
        }

        return map;
    }

    private Map<String, StorageSpacesInfo> getInfoFromWMIHelper(String pattern)
        throws StorageException
    {
        Map<String, StorageSpacesInfo> map = new HashMap<>();
        OutputData res;

        res = WmiHelper.run(extCmdFactory, new String[] {
            "virtual-disk",
            "list",
            pattern }
        );
        final String[] lines = StringUtils.split(new String(res.stdoutData, StandardCharsets.UTF_8), "\n");
        for (String line: lines)
        {
            final String[] data = StringUtils.split(line.trim(), "\t");

            if (data.length != 5)
            {       /* not created by LINSTOR, no partition 2, ignore */
                continue;
            }
            Long size;
            Long allocatedSize;
            String GUID;
            String fullIdentifier = data[2] + File.separator + data[1];
            String storPoolName = data[2];
            String diskName = data[1];

            GUID = data[4].replaceAll("[^a-fA-F0-9-]", "");
            try
            {
                size = StorageUtils.parseDecimalAsLong(data[3]);
                allocatedSize = StorageUtils.parseDecimalAsLong(data[0]);
            }
            catch (NumberFormatException exc)
            {
                throw new StorageException("Unable to parse size of VirtualDisk " + data[1],
                    "Size to parse: " + data[3] + " - " + data[0],
                    null,
                    null,
                    "cmd",
                    exc);
            }
            size = size / 1024;     /* LINSTOR computes in KiB not in bytes */
            allocatedSize = allocatedSize / 1024;

            StorageSpacesInfo info = new StorageSpacesInfo(size, allocatedSize, diskName, GUID, storPoolName);
            if (map.putIfAbsent(fullIdentifier, info) != null)
            {
                throw new StorageException("VirtualDisk " + diskName + " exists more than once in " + storPoolName + "? Please fix that.",
                    "fullIdentifier is " + fullIdentifier,
                    null,
                    null,
                    "cmd",
                    null);
            }
        }

        return map;
    }

    private void updateInfoListCache()
        throws StorageException
    {
        if (rebuildCache)
        {
            infoListCache.clear();
            infoListCache.putAll(getInfoFromWMIHelper("LINSTOR-%"));
            rebuildCache = false;
        }
        else
        {
            for (String volume: dirtyVolumes)
            {
                    /* arg to split is a regexp and seperator usually
                     * backslash (\) so we need to quote it.
                     */
                String[] arr = StringUtils.split(volume, "\\" + File.separator);
                String pattern = arr[1];
                infoListCache.remove(volume);

                Map<String, StorageSpacesInfo> maybeChanged = getInfoFromWMIHelper(pattern);
                infoListCache.putAll(maybeChanged);
            }
            dirtyVolumes.clear();
        }
    }

    @Override
    protected Map<String, StorageSpacesInfo> getInfoListImpl(
        List<StorageSpacesData<Resource>> vlmDataList,
        List<StorageSpacesData<Snapshot>> snapVlms
    )
        throws StorageException
    {
        updateInfoListCache();
        return infoListCache;
    }

    @Override
    public @Nullable String getDevicePath(String storageName, String lvId)
    {
        String identifier = storageName + File.separator + lvId;
        StorageSpacesInfo info = infoListCache.get(identifier);
        String ret = null;

        if (info == null)
        {
            try
            {
                updateInfoListCache();
            }
            catch (StorageException exc)
            {
                errorReporter.logWarning("Couldn't update info cache, exception is " + exc);
            }
            info = infoListCache.get(identifier);
        }

        if (info != null)
        {
            ret = info.getPath();
        }
        return ret;
    }

    @Override
    protected String asLvIdentifier(StorPoolName spName, ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
    {
        return String.format(
            "LINSTOR-%s%s_%05d",
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String asSnapLvIdentifier(StorageSpacesData<Snapshot> snapVlmDataRef)
    {
        return "";  /* not implemented */
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getStoragePoolFriendlyName(storPoolRef);
    }

    protected String getStoragePoolFriendlyName(StorPool storPool)
    {
        String name;
        try
        {
            name = StringUtils.split(DeviceLayerUtils.getNamespaceStorDriver(
                    storPool.getProps()
                )
                .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY), "/")[0];
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return name;
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPoolRef) throws StorageException
    {
        ReadOnlyProps props = storPoolRef.getReadOnlyProps();
        String poolName = props.getProp("StorPoolName", "StorDriver");
        OutputData res;
        Long totalSize;
        Long allocatedSize;

        res = WmiHelper.run(extCmdFactory, new String[] {
            "storage-pool",
            "get-sizes",
            poolName }
        );

        String line = new String(res.stdoutData, StandardCharsets.UTF_8).trim();
        String[] arr = StringUtils.split(line, " ");
        try
        {
            totalSize = Long.parseLong(arr[0]) / 1024L;
            allocatedSize = Long.parseLong(arr[1]) / 1024L;
        }
        catch (NumberFormatException | ArrayIndexOutOfBoundsException exc)
        {
            throw new StorageException(
                "Unable to parse Size output of Get-StoragePool",
                "WMIHelper output: '" + line + "'",
                null,
                null,
                "",
                exc
            );
        }
        return new SpaceInfo(totalSize, totalSize-allocatedSize);
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
        throws StorageException
    {
        return null;
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws DatabaseException, StorageException
    {
        return null;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return true;
    }

    @Override
    protected void setDevicePath(StorageSpacesData<Resource> vlmData, String devPath) throws DatabaseException
    {
        vlmData.setDevicePath(devPath);
    }

    @Override
    protected long getAllocatedSize(StorageSpacesData<Resource> vlmData)
        throws StorageException
    {
        final StorageSpacesInfo info = infoListCache.get(getFullQualifiedIdentifier(vlmData));
        try
        {
            updateInfo(vlmData, info);
        }
        catch (DatabaseException exc)
        {
            throw new StorageException("Unable to update info",
                    "Unable to update info",
                    null,
                    null,
                    "cmd",
                    exc);
        }
        return vlmData.getAllocatedSize();
    }

    @Override
    protected void deactivateLvImpl(StorageSpacesData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, DatabaseException
    {
        throw new StorageException("deactivateLvImpl not implemented",
            "deactivateLvImpl not implemented",
            null,
            null,
            "cmd",
            null);

    }

    @Override
    protected void createSnapshotForCloneImpl(
        StorageSpacesData<Resource> vlmData,
        String cloneRscName)
        throws StorageException
    {
        throw new StorageException("createLvWithCopyImpl not implemented",
            "createLvWithCopyImpl not implemented",
            null,
            null,
            "cmd",
            null);
    }

    @Override
    protected void setAllocatedSize(StorageSpacesData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(StorageSpacesData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(StorageSpacesData<Resource> vlmData, long size)
    {
        vlmData.setExpectedSize(size);
    }

    @Override
    protected String getStorageName(StorageSpacesData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getStoragePoolFriendlyName();
    }

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef)
    {
        /*
         * This is the extent size of the partition not
         * the one of storage spaces itself. The storage
         * spaces extent size would be 256*1024*1024 bytes.
         */
        return WORST_CASE_GRANULARITY;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.STORAGE_SPACES;
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }
}
