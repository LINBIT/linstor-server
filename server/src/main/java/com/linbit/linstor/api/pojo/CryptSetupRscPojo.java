package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class CryptSetupRscPojo implements RscLayerDataPojo
{
    private final int id;
    private final List<RscLayerDataPojo> children;
    private final String rscNameSuffix;

    private final List<CryptVlmPojo> vlms;

    public CryptSetupRscPojo(
        int idRef,
        List<RscLayerDataPojo> childrenRef,
        String rscNameSuffixRef,
        List<CryptVlmPojo> vlmsRef
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
        return DeviceLayerKind.CRYPT_SETUP;
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
    public List<CryptVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class CryptVlmPojo implements VlmLayerDataPojo
    {
        private final int vlmNr;
        private final byte[] encryptedPassword;
        private final String devicePath;
        private final String backingDevice;
        private final long allocatedSize;
        private final long usableSize;

        public CryptVlmPojo(
            int vlmNrRef,
            byte[] encryptedPasswordRef,
            String devicePathRef,
            String backingDeviceRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            vlmNr = vlmNrRef;
            encryptedPassword = encryptedPasswordRef;
            devicePath = devicePathRef;
            backingDevice = backingDeviceRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
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

        public String getDevicePath()
        {
            return devicePath;
        }

        public String getBackingDevice()
        {
            return backingDevice;
        }

        public long getAllocatedSize()
        {
            return allocatedSize;
        }

        public long getUsableSize()
        {
            return usableSize;
        }

        @Override
        public DeviceProviderKind getProviderKind()
        {
            return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
        }
    }
}
