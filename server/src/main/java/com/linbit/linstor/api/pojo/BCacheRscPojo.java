package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BCacheRscPojo implements RscLayerDataApi
{
    @JsonIgnore
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    @JsonIgnore
    private final boolean suspend;
    @JsonIgnore
    private final Set<LayerIgnoreReason> ignoreReasons;

    private final List<BCacheVlmPojo> vlms;

    public BCacheRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<BCacheVlmPojo> vlmsRef,
        boolean suspendRef,
        Set<LayerIgnoreReason> ignoreReasonRef
    )
    {
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = suspendRef;
        ignoreReasons = Collections.unmodifiableSet(ignoreReasonRef);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BCacheRscPojo(
        @JsonProperty("children") List<RscLayerDataApi> childrenRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("volumeList") List<BCacheVlmPojo> vlmsRef
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
        return DeviceLayerKind.BCACHE;
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

    @SuppressWarnings("unchecked")
    @Override
    public List<BCacheVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class BCacheVlmPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        @JsonIgnore
        private final @Nullable String devicePath;
        @JsonIgnore
        private final @Nullable String dataDevice;
        @JsonIgnore
        private final @Nullable String cacheDevice;
        private final @Nullable String cacheStorPoolName;
        @JsonIgnore
        private final long allocatedSize;
        @JsonIgnore
        private final long usableSize;
        @JsonIgnore
        private final @Nullable String diskState;
        @JsonIgnore
        private final long discGran;
        @JsonIgnore
        private final @Nullable UUID deviceUuid;
        @JsonIgnore
        private final boolean exists;

        public BCacheVlmPojo(
            int vlmNrRef,
            @Nullable String devicePathRef,
            @Nullable String dataDeviceRef,
            @Nullable String cacheDeviceRef,
            @Nullable String cacheStorPoolNameRef,
            long allocatedSizeRef,
            long usableSizeRef,
            @Nullable String diskStateRef,
            long discGranRef,
            @Nullable UUID deviceUuidRef,
            boolean existsRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            dataDevice = dataDeviceRef;
            cacheDevice = cacheDeviceRef;
            cacheStorPoolName = cacheStorPoolNameRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
            discGran = discGranRef;
            deviceUuid = deviceUuidRef;
            exists = existsRef;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public BCacheVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("cacheStorPoolName") String cacheStorPoolNameRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = null;
            dataDevice = null;
            cacheDevice = null;
            cacheStorPoolName = cacheStorPoolNameRef;
            allocatedSize = VlmProviderObject.UNINITIALIZED_SIZE;
            usableSize = VlmProviderObject.UNINITIALIZED_SIZE;
            diskState = null;
            discGran = VlmProviderObject.UNINITIALIZED_SIZE;
            deviceUuid = null;
            exists = false;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.BCACHE;
        }

        @Override
        public DeviceProviderKind getProviderKind()
        {
            return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
        }

        @Override
        public @Nullable String getDevicePath()
        {
            return devicePath;
        }

        public @Nullable String getDataDevice()
        {
            return dataDevice;
        }

        public @Nullable String getCacheDevice()
        {
            return cacheDevice;
        }

        public @Nullable String getCacheStorPoolName()
        {
            return cacheStorPoolName;
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
        public long getDiscGran()
        {
            return discGran;
        }

        public UUID getDeviceUuid()
        {
            return deviceUuid;
        }

        @Override
        public boolean exists()
        {
            return exists;
        }
    }
}
