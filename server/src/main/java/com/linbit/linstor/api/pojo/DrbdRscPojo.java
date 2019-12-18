package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class DrbdRscPojo implements RscLayerDataApi
{
    private final int id;
    private final List<RscLayerDataApi> children;
    private final String rscNameSuffix;

    private final DrbdRscDfnPojo drbdRscDfn;
    private final int nodeId;
    private final short peerSlots;
    private final int alStripes;
    private final long alStripeSize;
    private final long flags;
    private final List<DrbdVlmPojo> vlms;
    private final boolean suspend;

    public DrbdRscPojo(
        int idRef,
        List<RscLayerDataApi> childrenRef,
        String rscNameSuffixRef,
        DrbdRscDfnPojo drbdRscDfnRef,
        int nodeIdRef,
        short peerSlotsRef,
        int alStripesRef,
        long alStripeSizeRef,
        long flagsRef,
        List<DrbdVlmPojo> vlmsRef,
        boolean suspendRef
    )
    {
        id = idRef;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        drbdRscDfn = drbdRscDfnRef;
        nodeId = nodeIdRef;
        peerSlots = peerSlotsRef;
        alStripes = alStripesRef;
        alStripeSize = alStripeSizeRef;
        flags = flagsRef;
        vlms = vlmsRef;
        suspend = suspendRef;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.DRBD;
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

    public DrbdRscDfnPojo getDrbdRscDfn()
    {
        return drbdRscDfn;
    }

    public int getNodeId()
    {
        return nodeId;
    }

    public short getPeerSlots()
    {
        return peerSlots;
    }

    public int getAlStripes()
    {
        return alStripes;
    }

    public long getAlStripeSize()
    {
        return alStripeSize;
    }

    public long getFlags()
    {
        return flags;
    }

    @Override
    public boolean getSuspend()
    {
        return suspend;
    }

    @Override
    public List<DrbdVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class DrbdRscDfnPojo implements RscDfnLayerDataApi
    {
        private final String rscNameSuffix;
        private final short peerSlots;
        private final int alStripes;
        private final long alStripeSize;
        private final Integer port;
        private final String transportType;
        private final String secret;
        private final boolean down;

        public DrbdRscDfnPojo(
            String rscNameSuffixRef,
            short peerSlotsRef,
            int alStripesRef,
            long alStripeSizeRef,
            Integer portRef,
            String transportTypeRef,
            String secretRef,
            boolean downRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            peerSlots = peerSlotsRef;
            alStripes = alStripesRef;
            alStripeSize = alStripeSizeRef;
            port = portRef;
            transportType = transportTypeRef;
            secret = secretRef;
            down = downRef;
        }

        @Override
        public String getRscNameSuffix()
        {
            return rscNameSuffix;
        }

        public short getPeerSlots()
        {
            return peerSlots;
        }

        public int getAlStripes()
        {
            return alStripes;
        }

        public long getAlStripeSize()
        {
            return alStripeSize;
        }

        public Integer getPort()
        {
            return port;
        }

        public String getTransportType()
        {
            return transportType;
        }

        public String getSecret()
        {
            return secret;
        }

        public boolean isDown()
        {
            return down;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.DRBD;
        }
    }

    public static class DrbdVlmPojo implements VlmLayerDataApi
    {
        private final DrbdVlmDfnPojo drbdVlmDfn;
        private final String devicePath;
        private final String backingDisk;
        private final String externalMetaDataStorPool;
        private final String metaDisk;
        private final long allocatedSize;
        private final long usableSize;
        private final String diskState;

        public DrbdVlmPojo(
            DrbdVlmDfnPojo drbdVlmDfnRef,
            String devicePathRef,
            String backingDiskRef,
            String externalMetaDataStorPoolRef,
            String metaDiskRef,
            long allocatedSizeRef,
            long usableSizeRef,
            String diskStateRef
        )
        {
            drbdVlmDfn = drbdVlmDfnRef;
            devicePath = devicePathRef;
            backingDisk = backingDiskRef;
            externalMetaDataStorPool = externalMetaDataStorPoolRef;
            metaDisk = metaDiskRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
        }

        public DrbdVlmDfnPojo getDrbdVlmDfn()
        {
            return drbdVlmDfn;
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

        public String getExternalMetaDataStorPool()
        {
            return externalMetaDataStorPool;
        }

        public String getMetaDisk()
        {
            return metaDisk;
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
        public int getVlmNr()
        {
            return drbdVlmDfn.vlmNr;
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
            return DeviceLayerKind.DRBD;
        }
    }

    public static class DrbdVlmDfnPojo implements VlmDfnLayerDataApi
    {
        private final String rscNameSuffix;
        private final int vlmNr;
        private final Integer minorNr;

        public DrbdVlmDfnPojo(
            String rscNameSuffixRef,
            int vlmNrRef,
            Integer minorNrRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            vlmNr = vlmNrRef;
            minorNr = minorNrRef;
        }

        public String getRscNameSuffix()
        {
            return rscNameSuffix;
        }

        @Override
        public int getVlmNr()
        {
            return vlmNr;
        }

        public Integer getMinorNr()
        {
            return minorNr;
        }

        @Override
        public DeviceLayerKind getLayerKind()
        {
            return DeviceLayerKind.DRBD;
        }
    }
}
