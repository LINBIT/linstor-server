package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    private final Set<LayerIgnoreReason> ignoreReasons;

    private final List<LuksVlmPojo> vlms;

    public LuksRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<LuksVlmPojo> vlmsRef,
        boolean suspendRef,
        Set<LayerIgnoreReason> ignoreReasonsRef
    )
    {
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = suspendRef;
        ignoreReasons = Collections.unmodifiableSet(ignoreReasonsRef);
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
        ignoreReasons = null;
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
    public Set<LayerIgnoreReason> getIgnoreReasons()
    {
        return ignoreReasons;
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
        private final @Nullable byte[] modifyPassword;
        @JsonIgnore
        private final String devicePath;
        @JsonIgnore
        private final String dataDevice;
        @JsonIgnore
        private final long allocatedSize;
        @JsonIgnore
        private final long usableSize;
        @JsonIgnore
        private final boolean open;
        @JsonIgnore
        private final String diskState;
        @JsonIgnore
        private final long discGran;
        @JsonIgnore
        private final boolean exists;

        public LuksVlmPojo(
            int vlmNrRef,
            byte[] encryptedPasswordRef,
            String devicePathRef,
            String dataDeviceRef,
            long allocatedSizeRef,
            long usableSizeRef,
            boolean isOpenRef,
            String diskStateRef,
            long discGranRef,
            boolean existsRef,
            @Nullable byte[] modifyPasswordRef
        )
        {
            vlmNr = vlmNrRef;
            encryptedPassword = encryptedPasswordRef;
            devicePath = devicePathRef;
            dataDevice = dataDeviceRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            open = isOpenRef;
            diskState = diskStateRef;
            discGran = discGranRef;
            exists = existsRef;
            modifyPassword = modifyPasswordRef;
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
            dataDevice = null;
            allocatedSize = VlmProviderObject.UNINITIALIZED_SIZE;
            usableSize = VlmProviderObject.UNINITIALIZED_SIZE;
            open = false;
            diskState = null;
            discGran = VlmProviderObject.UNINITIALIZED_SIZE;
            exists = false;
            modifyPassword = null;
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

        public String getDataDevice()
        {
            return dataDevice;
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
        public long getDiscGran()
        {
            return discGran;
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

        public @Nullable byte[] getModifyPassword()
        {
            return modifyPassword;
        }

        @Override
        public boolean exists()
        {
            return exists;
        }
    }
}
