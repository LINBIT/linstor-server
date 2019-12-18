package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class LuksRscPojo implements RscLayerDataApi
{
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    private final boolean suspend;

    private final List<LuksVlmPojo> vlms;

    public LuksRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<LuksVlmPojo> vlmsRef,
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
        return DeviceLayerKind.LUKS;
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
    public List<LuksVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class LuksVlmPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        private final byte[] encryptedPassword;
        private final String devicePath;
        private final String backingDevice;
        private final long allocatedSize;
        private final long usableSize;
        private final boolean isOpen;
        private final String diskState;

        public LuksVlmPojo(
            int vlmNrRef,
            byte[] encryptedPasswordRef,
            String devicePathRef,
            String backingDeviceRef,
            long allocatedSizeRef,
            long usableSizeRef,
            boolean isOpenRef,
            String diskStateRef
        )
        {
            vlmNr = vlmNrRef;
            encryptedPassword = encryptedPasswordRef;
            devicePath = devicePathRef;
            backingDevice = backingDeviceRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            isOpen = isOpenRef;
            diskState = diskStateRef;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        public byte[] getEncryptedPassword()
        {
            return encryptedPassword;
        }

        @Override
        public String getDevicePath()
        {
            return devicePath;
        }

        public String getBackingDevice()
        {
            return backingDevice;
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

        public boolean isOpened()
        {
            return isOpen;
        }

        @Override
        public String getDiskState()
        {
            return diskState;
        }

        @Override
        public DeviceProviderKind getProviderKind()
        {
            return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.LUKS;
        }
    }
}
