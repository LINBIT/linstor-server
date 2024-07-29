package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.layer.LayerIgnoreReason;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DrbdRscPojo implements RscLayerDataApi
{
    @JsonIgnore
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
    @JsonIgnore
    private final boolean suspend;
    @JsonIgnore
    private final @Nullable Integer promotionScore;
    @JsonIgnore
    private final @Nullable Boolean mayPromote;
    @JsonIgnore
    private final Set<LayerIgnoreReason> ignoreReasons;

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
        boolean suspendRef,
        @Nullable Integer promotionScoreRef,
        @Nullable Boolean mayPromoteRef,
        Set<LayerIgnoreReason> ignoreReasonsRef
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
        promotionScore = promotionScoreRef;
        mayPromote = mayPromoteRef;
        ignoreReasons = Collections.unmodifiableSet(ignoreReasonsRef);
    }

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DrbdRscPojo(
        @JsonProperty("children") List<RscLayerDataApi> childrenRef,
        @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
        @JsonProperty("drbdRscDfn") DrbdRscDfnPojo drbdRscDfnRef,
        @JsonProperty("nodeId") int nodeIdRef,
        @JsonProperty("peerSlots") short peerSlotsRef,
        @JsonProperty("alStripes") int alStripesRef,
        @JsonProperty("alStripeSize") long alStripeSizeRef,
        @JsonProperty("flags") long flagsRef,
        @JsonProperty("volumeList") List<DrbdVlmPojo> vlmsRef
    )
    {
        id = BACK_DFLT_ID;
        children = childrenRef;
        rscNameSuffix = rscNameSuffixRef;
        drbdRscDfn = drbdRscDfnRef;
        nodeId = nodeIdRef;
        peerSlots = peerSlotsRef;
        alStripes = alStripesRef;
        alStripeSize = alStripeSizeRef;
        flags = flagsRef;
        vlms = vlmsRef;
        suspend = false;
        promotionScore = null;
        mayPromote = null;
        ignoreReasons = null;
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

    public @Nullable Integer getPromotionScore()
    {
        return promotionScore;
    }

    @Nullable
    public Boolean mayPromote()
    {
        return mayPromote;
    }

    @Override
    public Set<LayerIgnoreReason> getIgnoreReasons()
    {
        return ignoreReasons;
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
        @JsonIgnore
        private final @Nullable Integer port;
        private final String transportType;
        @JsonIgnore
        private final @Nullable String secret;
        @JsonIgnore
        private final boolean down;

        public DrbdRscDfnPojo(
            String rscNameSuffixRef,
            short peerSlotsRef,
            int alStripesRef,
            long alStripeSizeRef,
            @Nullable Integer portRef,
            String transportTypeRef,
            @Nullable String secretRef,
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

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DrbdRscDfnPojo(
            @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
            @JsonProperty("peerSlots") short peerSlotsRef,
            @JsonProperty("alStripes") int alStripesRef,
            @JsonProperty("alStripeSize") long alStripeSizeRef,
            @JsonProperty("transportType") String transportTypeRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            peerSlots = peerSlotsRef;
            alStripes = alStripesRef;
            alStripeSize = alStripeSizeRef;
            port = null;
            transportType = transportTypeRef;
            secret = null;
            down = false;
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
        @JsonIgnore
        private final @Nullable String devicePath;
        @JsonIgnore
        private final @Nullable String dataDevice;
        private final @Nullable String externalMetaDataStorPool;
        @JsonIgnore
        private final @Nullable String metaDevice;
        @JsonIgnore
        private final long allocatedSize;
        @JsonIgnore
        private final long usableSize;
        @JsonIgnore
        private final @Nullable String diskState;
        @JsonIgnore
        private final long discGran;
        @JsonIgnore
        private final boolean exists;

        public DrbdVlmPojo(
            DrbdVlmDfnPojo drbdVlmDfnRef,
            @Nullable String devicePathRef,
            @Nullable String dataDiskRef,
            @Nullable String externalMetaDataStorPoolRef,
            @Nullable String metaDeviceRef,
            long allocatedSizeRef,
            long usableSizeRef,
            @Nullable String diskStateRef,
            long discGranRef,
            boolean existsRef
        )
        {
            drbdVlmDfn = drbdVlmDfnRef;
            devicePath = devicePathRef;
            dataDevice = dataDiskRef;
            externalMetaDataStorPool = externalMetaDataStorPoolRef;
            metaDevice = metaDeviceRef;
            allocatedSize = allocatedSizeRef;
            usableSize = usableSizeRef;
            diskState = diskStateRef;
            discGran = discGranRef;
            exists = existsRef;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DrbdVlmPojo(
            @JsonProperty("vlmNr") int ignoredVlmNrRef,
            @JsonProperty("drbdVlmDfn") DrbdVlmDfnPojo drbdVlmDfnRef,
            @JsonProperty("externalMetaDataStorPool") String externalMetaDataStorPoolRef
        )
        {
            drbdVlmDfn = drbdVlmDfnRef;
            devicePath = null;
            dataDevice = null;
            externalMetaDataStorPool = externalMetaDataStorPoolRef;
            metaDevice = null;
            allocatedSize = VlmProviderObject.UNINITIALIZED_SIZE;
            usableSize = VlmProviderObject.UNINITIALIZED_SIZE;
            diskState = null;
            discGran = VlmProviderObject.UNINITIALIZED_SIZE;
            exists = false;
        }

        public DrbdVlmDfnPojo getDrbdVlmDfn()
        {
            return drbdVlmDfn;
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

        public @Nullable String getExternalMetaDataStorPool()
        {
            return externalMetaDataStorPool;
        }

        public @Nullable String getMetaDevice()
        {
            return metaDevice;
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
        public @Nullable String getDiskState()
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
            return DeviceLayerKind.DRBD;
        }

        @Override
        public boolean exists()
        {
            return exists;
        }
    }

    public static class DrbdVlmDfnPojo implements VlmDfnLayerDataApi
    {
        private final String rscNameSuffix;
        private final int vlmNr;
        @JsonIgnore
        private final @Nullable Integer minorNr;

        public DrbdVlmDfnPojo(
            String rscNameSuffixRef,
            int vlmNrRef,
            @Nullable Integer minorNrRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            vlmNr = vlmNrRef;
            minorNr = minorNrRef;
        }

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DrbdVlmDfnPojo(
            @JsonProperty("rscNameSuffix") String rscNameSuffixRef,
            @JsonProperty("vlmNr") int vlmNrRef
        )
        {
            rscNameSuffix = rscNameSuffixRef;
            vlmNr = vlmNrRef;
            minorNr = null;
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
