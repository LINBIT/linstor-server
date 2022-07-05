package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LuksRscPojo implements RscLayerDataApi
{
    @JsonIgnore
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    @JsonIgnore
    private final boolean suspend;
    @JsonIgnore
    private final @Nullable String ignoreReason;

    private final List<LuksVlmPojo> vlms;

    public LuksRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<LuksVlmPojo> vlmsRef,
        boolean suspendRef,
        @Nullable String ignoreReasonRef
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
    public LuksRscPojo(
        @JsonProperty("children") List<RscLayerDataApi> childrenRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("volumeList") List<LuksVlmPojo> vlmsRef
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
    public @Nullable String getIgnoreReason()
    {
        return ignoreReason;
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
        @JsonIgnore
        private final String devicePath;
        @JsonIgnore
        private final String backingDevice;
        @JsonIgnore
        private final long allocatedSize;
        @JsonIgnore
        private final long usableSize;
        @JsonIgnore
        private final boolean open;
        @JsonIgnore
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
            open = isOpenRef;
            diskState = diskStateRef;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public LuksVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("encryptedPassword") byte[] encryptedPasswordRef
        )
        {
            vlmNr = vlmNrRef;
            encryptedPassword = encryptedPasswordRef;
            devicePath = null;
            backingDevice = null;
            allocatedSize = VlmProviderObject.UNINITIALIZED_SIZE;
            usableSize = VlmProviderObject.UNINITIALIZED_SIZE;
            open = false;
            diskState = null;
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

        public boolean isOpen()
        {
            return open;
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
