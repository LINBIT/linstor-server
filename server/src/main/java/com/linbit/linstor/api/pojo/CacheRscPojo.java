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

public class CacheRscPojo implements RscLayerDataApi
{
    @JsonIgnore
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    @JsonIgnore
    private final boolean suspend;
    @JsonIgnore
    private final @Nullable String ignoreReason;

    private final List<CacheVlmPojo> vlms;

    public CacheRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<CacheVlmPojo> vlmsRef,
        boolean suspendRef,
        @Nullable String ignoreReasonRef
    )
    {
        super();
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = suspendRef;
        ignoreReason = ignoreReasonRef;
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CacheRscPojo(
        @JsonProperty("children") List<RscLayerDataApi> childrenRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("volumeList") List<CacheVlmPojo> vlmsRef
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
        return DeviceLayerKind.CACHE;
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
    public List<CacheVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class CacheVlmPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        @JsonIgnore
        private final String devicePath;
        @JsonIgnore
        private final String dataDevice;
        @JsonIgnore
        private final String cacheDevice;
        @JsonIgnore
        private final String metaDevice;
        private final String cacheStorPoolName;
        private final String metaStorPoolName;
        @JsonIgnore
        private final long allocatedSize;
        @JsonIgnore
        private final long usableSize;
        @JsonIgnore
        private final String diskState;
        @JsonIgnore
        private final long discGran;
        @JsonIgnore
        private final boolean exists;

        public CacheVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            String dataDeviceRef,
            String cacheDeviceRef,
            String metaDeviceRef,
            String cacheStorPoolNameRef,
            String metaStorPoolNameRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            long discGranRef,
            boolean existsRef
        )
        {
            super();
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            dataDevice = dataDeviceRef;
            cacheDevice = cacheDeviceRef;
            metaDevice = metaDeviceRef;
            cacheStorPoolName = cacheStorPoolNameRef;
            metaStorPoolName = metaStorPoolNameRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
            discGran = discGranRef;
            exists = existsRef;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public CacheVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("cacheStorPoolName") String cacheStorPoolNameRef,
            @JsonProperty("metaStorPoolName") String metaStorPoolNameRef
        )
        {
            super();
            vlmNr = vlmNrRef;
            devicePath = null;
            dataDevice = null;
            cacheDevice = null;
            metaDevice = null;
            cacheStorPoolName = cacheStorPoolNameRef;
            metaStorPoolName = metaStorPoolNameRef;
            allocatedSize = VlmProviderObject.UNINITIALIZED_SIZE;
            usableSize = VlmProviderObject.UNINITIALIZED_SIZE;
            diskState = null;
            discGran = VlmProviderObject.UNINITIALIZED_SIZE;
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
            return DeviceLayerKind.CACHE;
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

        public String getDataDevice()
        {
            return dataDevice;
        }

        public String getCacheDevice()
        {
            return cacheDevice;
        }

        public String getMetaDevice()
        {
            return metaDevice;
        }

        public String getCacheStorPoolName()
        {
            return cacheStorPoolName;
        }

        public String getMetaStorPoolName()
        {
            return metaStorPoolName;
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

        @Override
        public boolean exists()
        {
            return exists;
        }
    }

}
