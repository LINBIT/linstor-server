package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.interfaces.RscLayerDataPojo;
import com.linbit.linstor.api.interfaces.VlmLayerDataPojo;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class DrbdRscPojo implements RscLayerDataPojo
{
    private final int id;
    private final List<RscLayerDataPojo> children;
    private final String rscNameSuffix;

    private final DrbdRscDfnPojo drbdRscDfn;
    private final int nodeId;
    private final short peerSlots;
    private final int alStripes;
    private final long alStripeSize;
    private final long flags;
    private final List<DrbdVlmPojo> vlms;

    public DrbdRscPojo(
        int idRef,
        List<RscLayerDataPojo> childrenRef,
        String rscNameSuffixRef,
        DrbdRscDfnPojo drbdRscDfnRef,
        int nodeIdRef,
        short peerSlotsRef,
        int alStripesRef,
        long alStripeSizeRef,
        long flagsRef,
        List<DrbdVlmPojo> vlmsRef
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
    public List<RscLayerDataPojo> getChildren()
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
    public List<DrbdVlmPojo> getVolumeList()
    {
        return vlms;
    }

    public static class DrbdRscDfnPojo
    {
        private final String rscNameSuffix;
        private final short peerSlots;
        private final int alStripes;
        private final long alStripeSize;
        private final int port;
        private final String transportType;
        private final String secret;

        public DrbdRscDfnPojo(
            String rscNameSuffixRef,
            short peerSlotsRef,
            int alStripesRef,
            long alStripeSizeRef,
            int portRef,
            String transportTypeRef,
            String secretRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            peerSlots = peerSlotsRef;
            alStripes = alStripesRef;
            alStripeSize = alStripeSizeRef;
            port = portRef;
            transportType = transportTypeRef;
            secret = secretRef;
        }

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

        public int getPort()
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
    }

    public static class DrbdVlmPojo implements VlmLayerDataPojo
    {
        private final DrbdVlmDfnPojo drbdVlmDfn;
        private final String devicePath;
        private final String backingDisk;
        private final String metaDisk;
        private final long allocatedSize;
        private final long usableSize;

        public DrbdVlmPojo(
            DrbdVlmDfnPojo drbdVlmDfnRef,
            String devicePathRef,
            String backingDiskRef,
            String metaDiskRef,
            long allocatedSizeRef,
            long usableSizeRef
        )
        {
            super();
            drbdVlmDfn = drbdVlmDfnRef;
            devicePath = devicePathRef;
            backingDisk = backingDiskRef;
            metaDisk = metaDiskRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
        }
        public DrbdVlmDfnPojo getDrbdVlmDfn()
        {
            return drbdVlmDfn;
        }

        public String getDevicePath()
        {
            return devicePath;
        }

        public String getBackingDisk()
        {
            return backingDisk;
        }

        public String getMetaDisk()
        {
            return metaDisk;
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
        public int getVlmNr()
        {
            return drbdVlmDfn.vlmNr;
        }

        @Override
        public DeviceProviderKind getProviderKind()
        {
            return DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER;
        }
    }

    public static class DrbdVlmDfnPojo
    {
        private final String rscNameSuffix;
        private final int vlmNr;
        private final int minorNr;

        public DrbdVlmDfnPojo(
            String rscNameSuffixRef,
            int vlmNrRef,
            int minorNrRef
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

        public int getVlmNr()
        {
            return vlmNr;
        }

        public int getMinorNr()
        {
            return minorNr;
        }
    }
}
