package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.DISKLESS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.EXOS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.FILE;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.FILE_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.SPDK;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS_THIN;

import java.util.List;

public class StorageRscPojo implements RscLayerDataApi
{
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    private final boolean suspend;

    private final List<VlmLayerDataApi> vlms;

    public StorageRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<VlmLayerDataApi> vlmsRef,
        boolean suspendRef
    )
    {
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = suspendRef;
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
    public List<VlmLayerDataApi> getVolumeList()
    {
        return vlms;
    }

    private abstract static class AbsVlmProviderPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        private final String devicePath;
        private final long allocatedSize;
        private final long usableSize;
        private final String diskState;
        private final DeviceProviderKind kind;
        private final StorPoolApi storPoolApi;

        AbsVlmProviderPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef,
            DeviceProviderKind kindRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
            storPoolApi = storPoolApiRef;
            kind = kindRef;
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

        @Override
        public long getUsableSize()
        {
            return usableSize;
        }

        @Override
        public String getDiskState()
        {
            return diskState;
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
    }

    public static class DisklessVlmPojo extends AbsVlmProviderPojo
    {
        public DisklessVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, DISKLESS);
        }
    }

    public static class LvmVlmPojo extends AbsVlmProviderPojo
    {
        public LvmVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, LVM);
        }
    }

    public static class SpdkVlmPojo extends AbsVlmProviderPojo
    {
        public SpdkVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, SPDK);
        }
    }

    public static class LvmThinVlmPojo extends AbsVlmProviderPojo
    {
        public LvmThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, LVM_THIN);
        }
    }

    public static class ZfsVlmPojo extends AbsVlmProviderPojo
    {
        public ZfsVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, ZFS);
        }
    }

    public static class ZfsThinVlmPojo extends AbsVlmProviderPojo
    {
        public ZfsThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, ZFS_THIN);
        }
    }

    public static class FileVlmPojo extends AbsVlmProviderPojo
    {
        public FileVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, FILE);
        }
    }

    public static class FileThinVlmPojo extends AbsVlmProviderPojo
    {
        public FileThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, FILE_THIN);
        }
    }

    public static class ExosVlmPojo extends AbsVlmProviderPojo
    {
        public ExosVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, storPoolApiRef, EXOS);
        }
    }
}
