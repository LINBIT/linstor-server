package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;

import java.util.List;
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

    private final List<BCacheVlmPojo> vlms;

    public BCacheRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<BCacheVlmPojo> vlmsRef,
        boolean suspendRef
    )
    {
        super();
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = suspendRef;
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public BCacheRscPojo(
        @JsonProperty("children") List<RscLayerDataApi> childrenRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("volumeList") List<BCacheVlmPojo> vlmsRef
    )
    {
        super();
        id = BACK_DFLT_ID;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        vlms = vlmsRef;
        suspend = false;
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
        private final String devicePathData;
        @JsonIgnore
        private final String devicePathCache;
        private final String cacheStorPoolName;
        @JsonIgnore
        private final long allocatedSize;
        @JsonIgnore
        private final long usableSize;
        @JsonIgnore
        private final String diskState;
        @JsonIgnore
        private final @Nullable UUID deviceUuid;

        public BCacheVlmPojo(
            int vlmNrRef,
            String devicePathDataRef,
            String devicePathCacheRef,
            String cacheStorPoolNameRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            @Nullable UUID deviceUuidRef
        )
        {
            super();
            vlmNr = vlmNrRef;
            devicePathData = devicePathDataRef;
            devicePathCache = devicePathCacheRef;
            cacheStorPoolName = cacheStorPoolNameRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
            deviceUuid = deviceUuidRef;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public BCacheVlmPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("cacheStorPoolName") String cacheStorPoolNameRef
        )
        {
            super();
            vlmNr = vlmNrRef;
            devicePathData = null;
            devicePathCache = null;
            cacheStorPoolName = cacheStorPoolNameRef;
            allocatedSize = VlmProviderObject.UNINITIALIZED_SIZE;
            usableSize = VlmProviderObject.UNINITIALIZED_SIZE;
            diskState = null;
            deviceUuid = null;
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
        public String getDevicePath()
        {
            return devicePathData;
        }

        public String getDevicePathCache()
        {
            return devicePathCache;
        }

        public String getCacheStorPoolName()
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

        public UUID getDeviceUuid()
        {
            return deviceUuid;
        }
    }
}
