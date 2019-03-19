package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.DISKLESS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.LVM_THIN;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.SWORDFISH_INITIATOR;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.SWORDFISH_TARGET;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS;
import static com.linbit.linstor.storage.kinds.DeviceProviderKind.ZFS_THIN;

import java.util.List;

public class StorageRscPojo implements RscLayerDataApi
{
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;

    private final List<VlmLayerDataApi> vlms;

    public StorageRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<VlmLayerDataApi> vlmsRef
    )
    {
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
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

        AbsVlmProviderPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            DeviceProviderKind kindRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
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
            String diskStateRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, DISKLESS);
        }
    }

    public static class LvmVlmPojo extends AbsVlmProviderPojo
    {
        public LvmVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, LVM);
        }
    }

    public static class LvmThinVlmPojo extends AbsVlmProviderPojo
    {
        public LvmThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, LVM_THIN);
        }
    }

    public static class ZfsVlmPojo extends AbsVlmProviderPojo
    {
        public ZfsVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, ZFS);
        }
    }

    public static class ZfsThinVlmPojo extends AbsVlmProviderPojo
    {
        public ZfsThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, diskStateRef, ZFS_THIN);
        }
    }

    public static class SwordfishTargetVlmPojo extends AbsVlmProviderPojo
    {
        private final SwordfishVlmDfnPojo vlmDfn;

        public SwordfishTargetVlmPojo(
            SwordfishVlmDfnPojo vlmDfnRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmDfnRef.vlmNr, null, allocatedSizeRef, usableSizeRef, null, SWORDFISH_TARGET);
            vlmDfn = vlmDfnRef;
        }

        public SwordfishVlmDfnPojo getVlmDfn()
        {
            return vlmDfn;
        }
    }

    public static class SwordfishInitiatorVlmPojo extends AbsVlmProviderPojo
    {
        private final SwordfishVlmDfnPojo vlmDfn;

        public SwordfishInitiatorVlmPojo(
            SwordfishVlmDfnPojo vlmDfnRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            super(
                vlmDfnRef.vlmNr,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                diskStateRef,
                SWORDFISH_INITIATOR
            );
            vlmDfn = vlmDfnRef;
        }

        public SwordfishVlmDfnPojo getVlmDfn()
        {
            return vlmDfn;
        }
    }

    public static class SwordfishVlmDfnPojo implements VlmDfnLayerDataApi
    {
        private final String rscNameSuffix;
        private final int vlmNr;
        private final String vlmOdata;

        public SwordfishVlmDfnPojo(
            String rscNameSuffixRef,
            int vlmNrRef,
            String vlmOdataRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            vlmNr = vlmNrRef;
            vlmOdata = vlmOdataRef;
        }

        public String getSuffixedRscName()
        {
            return rscNameSuffix;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        public String getVlmOdata()
        {
            return vlmOdata;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return STORAGE;
        }
    }
}
