package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class CacheRscPojo implements RscLayerDataApi
{
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;
    private final boolean suspend;

    private final List<CacheVlmPojo> vlms;

    public CacheRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        List<CacheVlmPojo> vlmsRef,
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
    public List<CacheVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class CacheVlmPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        private final String devicePathData;
        private final String devicePathCache;
        private final String devicePathMeta;
        private final String cacheStorPoolName;
        private final String metaStorPoolName;
        private final long allocatedSize;
        private final long usableSize;
        private final String diskState;

        public CacheVlmPojo(
            int vlmNrRef,
            String devicePathDataRef,
            String devicePathCacheRef,
            String devicePathMetaRef,
            String cacheStorPoolNameRef,
            String metaStorPoolNameRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            super();
            vlmNr = vlmNrRef;
            devicePathData = devicePathDataRef;
            devicePathCache = devicePathCacheRef;
            devicePathMeta = devicePathMetaRef;
            cacheStorPoolName = cacheStorPoolNameRef;
            metaStorPoolName = metaStorPoolNameRef;
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
            return devicePathData;
        }

        public String getDevicePathCache()
        {
            return devicePathCache;
        }

        public String getDevicePathMeta()
        {
            return devicePathMeta;
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
    }

}
