package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.DISKLESS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.EBS_INIT;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.EXOS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.FILE;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.FILE_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.REMOTE_SPDK;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.SPDK;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.STORAGE_SPACES;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.STORAGE_SPACES_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS_THIN;

import javax.annotation.Nullable;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRscPojo implements RscLayerDataApi
{
    @JsonIgnore
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    @JsonIgnore
    private final boolean suspend;
    @JsonIgnore
    private final LayerIgnoreReason ignoreReason;

    private final List<VlmLayerDataApi> vlms;

    public StorageRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<VlmLayerDataApi> vlmsRef,
        boolean suspendRef,
        LayerIgnoreReason ignoreReasonRef
    )
    {
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = suspendRef;
        ignoreReason = ignoreReasonRef;
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public StorageRscPojo(
        @JsonProperty("children") List<RscLayerDataApi> childrenRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("volumeList") List<VlmLayerDataApi> vlmsRef
    )
    {
        id = BACK_DFLT_ID;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = false;
        ignoreReason = null;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return STORAGE;
    }

    @Override
    public int getId()
    {
        return id;
    }

    @Override
    public List<RscLayerDataApi> getChildren()
    {
        return children;
    }

    @Override
    public String getRscNameSuffix()
    {
        return rscNameSuffix;
    }

    @Override
    public boolean getSuspend()
    {
        return suspend;
    }

    @Override
    public LayerIgnoreReason getIgnoreReason()
    {
        return ignoreReason;
    }

    @Override
    public List<VlmLayerDataApi> getVolumeList()
    {
        return vlms;
    }

    private abstract static class AbsVlmProviderPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        @JsonIgnore
        private final String devicePath;
        private final long allocatedSize;
        private final long usableSize;
        private final Long snapAllocatedSize;
        private final Long snapUsableSize;
        @JsonIgnore
        private final String diskState;
        @JsonIgnore
        private final DeviceProviderKind kind;
        private final StorPoolApi storPoolApi;
        @JsonIgnore
        private final long discGran;
        @JsonIgnore
        private final boolean exists;
        private final @Nullable Long extentSize;

        AbsVlmProviderPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            DeviceProviderKind kindRef,
            boolean existsRef,
            @Nullable Long extentSizeRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            snapAllocatedSize = snapAllocatedSizeRef;
            snapUsableSize = snapUsableSizeRef;
            diskState = diskStateRef;
            discGran = discGranRef;
            storPoolApi = storPoolApiRef;
            kind = kindRef;
            exists = existsRef;
            extentSize = extentSizeRef;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public String getDevicePath()
        {
            return devicePath;
        }

        @Override
        public long getAllocatedSize()
        {
            return allocatedSize;
        }

        @JsonIgnore(false)
        @Override
        public long getUsableSize()
        {
            return usableSize;
        }

        @Override
        public Long getSnapshotAllocatedSize()
        {
            return snapAllocatedSize;
        }

        @Override
        public Long getSnapshotUsableSize()
        {
            return snapUsableSize;
        }

        @Override
        public String getDiskState()
        {
            return diskState;
        }

        @Override
        public long getDiscGran()
        {
            return discGran;
        }

        @Override
        public DeviceProviderKind getProviderKind()
        {
            return kind;
        }

        @Override
        public StorPoolApi getStorPoolApi()
        {
            return storPoolApi;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return STORAGE;
        }

        @Override
        public boolean exists()
        {
            return exists;
        }

        @Override
        public @Nullable Long getExtentSize()
        {
            return extentSize;
        }
    }

    public static class DisklessVlmPojo extends AbsVlmProviderPojo
    {
        public DisklessVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                DISKLESS,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DisklessVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef

        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                DISKLESS,
                false,
                null
            );
        }
    }

    public static class LvmVlmPojo extends AbsVlmProviderPojo
    {
        public LvmVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                LVM,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public LvmVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                LVM,
                false,
                null
            );
        }
    }

    public static class StorageSpacesVlmPojo extends AbsVlmProviderPojo
    {
        public StorageSpacesVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                64,
                storPoolApiRef,
                STORAGE_SPACES,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public StorageSpacesVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                64,
                storPoolApiRef,
                STORAGE_SPACES,
                false,
                null
            );
        }
    }

    public static class StorageSpacesThinVlmPojo extends AbsVlmProviderPojo
    {
        public StorageSpacesThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                64,
                storPoolApiRef,
                STORAGE_SPACES_THIN,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public StorageSpacesThinVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                64,
                storPoolApiRef,
                STORAGE_SPACES_THIN,
                false,
                null
            );
        }
    }

    public static class SpdkVlmPojo extends AbsVlmProviderPojo
    {
        public SpdkVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                SPDK,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public SpdkVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                SPDK,
                false,
                null
            );
        }
    }

    public static class RemoteSpdkVlmPojo extends AbsVlmProviderPojo
    {
        public RemoteSpdkVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                REMOTE_SPDK,
                existsRef,
                null
            );
        }
    }

    public static class LvmThinVlmPojo extends AbsVlmProviderPojo
    {
        public LvmThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                LVM_THIN,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public LvmThinVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                LVM_THIN,
                false,
                null
            );
        }
    }

    public static class ZfsVlmPojo extends AbsVlmProviderPojo
    {
        public ZfsVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef,
            @Nullable Long extentSizeRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                ZFS,
                existsRef,
                extentSizeRef
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ZfsVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef,
            @JsonProperty("extentSize") @Nullable Long extentSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                ZFS,
                false,
                extentSizeRef
            );
        }
    }

    public static class ZfsThinVlmPojo extends AbsVlmProviderPojo
    {
        public ZfsThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef,
            @Nullable Long extentSizeRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                ZFS_THIN,
                existsRef,
                extentSizeRef
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public ZfsThinVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef,
            @JsonProperty("extentSize") @Nullable Long extentSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                ZFS_THIN,
                false,
                extentSizeRef
            );
        }
    }

    public static class FileVlmPojo extends AbsVlmProviderPojo
    {
        public FileVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                FILE,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public FileVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                FILE,
                false,
                null
            );
        }
    }

    public static class FileThinVlmPojo extends AbsVlmProviderPojo
    {
        public FileThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                FILE_THIN,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public FileThinVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                FILE_THIN,
                false,
                null
            );
        }
    }

    @Deprecated(forRemoval = true)
    public static class ExosVlmPojo extends AbsVlmProviderPojo
    {
        public ExosVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                EXOS,
                existsRef,
                null
            );
        }
    }

    @Deprecated(forRemoval = true)
    public static class EbsVlmPojo extends AbsVlmProviderPojo
    {
        public EbsVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            Long snapAllocatedSizeRef,
            Long snapUsableSizeRef,
            String diskStateRef,
            long discGranRef,
            StorPoolApi storPoolApiRef,
            boolean existsRef
        )
        {
            super(
                vlmNrRef,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                diskStateRef,
                discGranRef,
                storPoolApiRef,
                EBS_INIT,
                existsRef,
                null
            );
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public EbsVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolApi") StorPoolApi storPoolApiRef,
            @JsonProperty("usableSize") long usableSizeRef,
            @JsonProperty("allocatedSize") long allocatedSizeRef,
            @JsonProperty("snapshotUsableSize") Long snapUsableSizeRef,
            @JsonProperty("snapshotAllocatedSize") Long snapAllocatedSizeRef
        )
        {
            super(
                vlmNrRef,
                null,
                allocatedSizeRef,
                usableSizeRef,
                snapAllocatedSizeRef,
                snapUsableSizeRef,
                null,
                VlmProviderObject.UNINITIALIZED_SIZE,
                storPoolApiRef,
                EBS_INIT,
                false,
                null
            );
        }
    }
}
