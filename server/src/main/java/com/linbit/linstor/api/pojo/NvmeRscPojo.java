package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class NvmeRscPojo implements RscLayerDataApi
{
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    private final List<NvmeVlmPojo> vlms;
    private final boolean suspend;

    public NvmeRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<NvmeVlmPojo> vlmsRef,
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
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.NVME;
    }

    @Override
    public boolean getSuspend()
    {
        return suspend;
    }

    @Override
    public List<NvmeVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class NvmeVlmPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        private final String devicePath;
        private final String backingDisk;
        private final long allocatedSize;
        private final long usableSize;
        private final String diskState;

        public NvmeVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            String backingDiskRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            backingDisk = backingDiskRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.NVME;
        }

        @Override
        public DeviceProviderKind getProviderKind()
        {
            return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
        }

        @Override
        public String getDevicePath()
        {
            return devicePath;
        }

        public String getBackingDisk()
        {
            return backingDisk;
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
    }
}
