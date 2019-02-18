package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class StorageRscPojo implements RscLayerDataPojo
{
    private final int id;
    private final List<RscLayerDataPojo> children;
    private final String rscNameSuffix;

    private final List<VlmLayerDataPojo> vlms;

    public StorageRscPojo(
        int idRef,
        List<RscLayerDataPojo> childrenRef,
        String rscNameSuffixRef,
        List<VlmLayerDataPojo> vlmsRef
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
        return DeviceLayerKind.STORAGE;
    }

    @Override
    public int getId()
    {
        return id;
    }

    @Override
    public List<RscLayerDataPojo> getChildren()
    {
        return children;
    }

    @Override
    public String getRscNameSuffix()
    {
        return rscNameSuffix;
    }

    @Override
    public List<VlmLayerDataPojo> getVolumeList()
    {
        return vlms;
    }

    private static abstract class AbsVlmPojo implements VlmLayerDataPojo
    {
        private final int vlmNr;
        private final String devicePath;
        private final long allocatedSize;
        private final long usableSize;
        private final DeviceProviderKind kind;

        AbsVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef,
            DeviceProviderKind kindRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
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
        public DeviceProviderKind getProviderKind()
        {
            return kind;
        }
    }

    public static class DrbdDisklessVlmPojo extends AbsVlmPojo
    {
        public DrbdDisklessVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, DeviceProviderKind.DRBD_DISKLESS);
        }
    }

    public static class LvmVlmPojo extends AbsVlmPojo
    {
        public LvmVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, DeviceProviderKind.LVM);
        }
    }

    public static class LvmThinVlmPojo extends AbsVlmPojo
    {
        public LvmThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, DeviceProviderKind.LVM_THIN);
        }
    }

    public static class ZfsVlmPojo extends AbsVlmPojo
    {
        public ZfsVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, DeviceProviderKind.ZFS);
        }
    }

    public static class ZfsThinVlmPojo extends AbsVlmPojo
    {
        public ZfsThinVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmNrRef, devicePathRef, allocatedSizeRef, usableSizeRef, DeviceProviderKind.ZFS_THIN);
        }
    }

    public static class SwordfishTargetVlmPojo extends AbsVlmPojo
    {
        private final SwordfishVlmDfnPojo vlmDfn;

        public SwordfishTargetVlmPojo(
            SwordfishVlmDfnPojo vlmDfnRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(vlmDfnRef.vlmNr, null, allocatedSizeRef, usableSizeRef, DeviceProviderKind.SWORDFISH_TARGET);
            vlmDfn = vlmDfnRef;
        }

        public SwordfishVlmDfnPojo getVlmDfn()
        {
            return vlmDfn;
        }
    }

    public static class SwordfishInitiatorVlmPojo extends AbsVlmPojo
    {
        private final SwordfishVlmDfnPojo vlmDfn;

        public SwordfishInitiatorVlmPojo(
            SwordfishVlmDfnPojo vlmDfnRef,
            String devicePathRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super(
                vlmDfnRef.vlmNr,
                devicePathRef,
                allocatedSizeRef,
                usableSizeRef,
                DeviceProviderKind.SWORDFISH_INITIATOR
            );
            vlmDfn = vlmDfnRef;
        }

        public SwordfishVlmDfnPojo getVlmDfn()
        {
            return vlmDfn;
        }
    }

    public static class SwordfishVlmDfnPojo
    {
        private final String suffixedRscName;
        private final int vlmNr;
        private final String vlmOdata;

        public SwordfishVlmDfnPojo(
            String suffixedRscNameRef,
            int vlmNrRef,
            String vlmOdataRef
        )
        {
            suffixedRscName = suffixedRscNameRef;
            vlmNr = vlmNrRef;
            vlmOdata = vlmOdataRef;
        }

        public String getSuffixedRscName()
        {
            return suffixedRscName;
        }

        public int getVlmNr()
        {
            return vlmNr;
        }

        public String getVlmOdata()
        {
            return vlmOdata;
        }
    }
}
