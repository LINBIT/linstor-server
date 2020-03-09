package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collections;
import java.util.List;

public class OpenflexRscPojo implements RscLayerDataApi
{
    private final int id;
    private final OpenflexRscDfnPojo rscDfn;
    private final List<OpenflexVlmPojo> vlms;
    private final boolean suspend;

    public OpenflexRscPojo(
        int idRef,
        OpenflexRscDfnPojo rscDfnRef,
        List<OpenflexVlmPojo> vlmsRef,
        boolean suspendRef
    )
    {
        id = idRef;
        rscDfn = rscDfnRef;
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
        return Collections.emptyList(); // like storage, this layer will never have children
    }

    @Override
    public String getRscNameSuffix()
    {
        return rscDfn.resourceNameSuffix;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.OPENFLEX;
    }

    @Override
    public boolean getSuspend()
    {
        return suspend;
    }

    @Override
    public List<OpenflexVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public OpenflexRscDfnPojo getOpenflexRscDfn()
    {
        return rscDfn;
    }

    public static class OpenflexRscDfnPojo implements RscDfnLayerDataApi
    {
        private final String resourceNameSuffix;
        private final String nqn;

        public OpenflexRscDfnPojo(
            String resourceNameSuffixRef,
            String nqnRef
        )
        {
            resourceNameSuffix = resourceNameSuffixRef;
            nqn = nqnRef;
        }

        @Override
        public String getRscNameSuffix()
        {
            return resourceNameSuffix;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.OPENFLEX;
        }

        public String getNqn()
        {
            return nqn;
        }
    }

    public static class OpenflexVlmPojo implements VlmLayerDataApi
    {
        private final int vlmNr;
        private final String devicePath;
        private final String openflexId;
        private final long allocatedSize;
        private final long usableSize;
        private final String diskState;
        private final StorPoolApi storPoolApi;

        public OpenflexVlmPojo(
            int vlmNrRef,
            String devicePathRef,
            String openflexIdRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef,
            StorPoolApi storPoolApiRef
        )
        {
            vlmNr = vlmNrRef;
            devicePath = devicePathRef;
            openflexId = openflexIdRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
            storPoolApi = storPoolApiRef;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.OPENFLEX;
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

        public String getOpenflexId()
        {
            return openflexId;
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
        public StorPoolApi getStorPoolApi()
        {
            return storPoolApi;
        }
    }

}
