package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DrbdLayerMetaPojo
{
    private final short rdPeerSlots;
    private final int rdAlStripes;
    private final long rdAlStripeSize;
    private final short rscPeerSlots;
    private final int rscAlStripes;
    private final long rscAlStripeSize;
    private final int rscNodeId;
    private final long rscFlags;
    private final Map<Integer, DrbdLayerVlmMetaPojo> vlmsMap;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DrbdLayerMetaPojo(
        @JsonProperty("rdPeerSlots") short rdPeerSlotsRef,
        @JsonProperty("rdAlStripes") int rdAlStripesRef,
        @JsonProperty("rdAlStripeSize") long rdAlStripeSizeRef,
        @JsonProperty("rscPeerSlots") short rscPeerSlotsRef,
        @JsonProperty("rscAlStripes") int rscAlStripesRef,
        @JsonProperty("rscAlStripeSize") long rscAlStripeSizeRef,
        @JsonProperty("rscNodeId") int rscNodeIdRef,
        @JsonProperty("rscFlags") long rscFlagsRef,
        @JsonProperty("vlmsMap") Map<Integer, DrbdLayerVlmMetaPojo> vlmsMapRef
    )
    {
        rdPeerSlots = rdPeerSlotsRef;
        rdAlStripes = rdAlStripesRef;
        rdAlStripeSize = rdAlStripeSizeRef;
        rscPeerSlots = rscPeerSlotsRef;
        rscAlStripes = rscAlStripesRef;
        rscAlStripeSize = rscAlStripeSizeRef;
        rscNodeId = rscNodeIdRef;
        rscFlags = rscFlagsRef;
        vlmsMap = vlmsMapRef;
    }

    public short getRdPeerSlots()
    {
        return rdPeerSlots;
    }

    public int getRdAlStripes()
    {
        return rdAlStripes;
    }

    public long getRdAlStripeSize()
    {
        return rdAlStripeSize;
    }

    public short getRscPeerSlots()
    {
        return rscPeerSlots;
    }

    public int getRscAlStripes()
    {
        return rscAlStripes;
    }

    public long getRscAlStripeSize()
    {
        return rscAlStripeSize;
    }

    public int getRscNodeId()
    {
        return rscNodeId;
    }

    public long getRscFlags()
    {
        return rscFlags;
    }

    public Map<Integer, DrbdLayerVlmMetaPojo> getVlmsMap()
    {
        return vlmsMap;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class DrbdLayerVlmMetaPojo
    {
        private final int vlmNr;
        private final String extStorPoolName;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public DrbdLayerVlmMetaPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("extStorPoolName") String extStorPoolNameRef
        )
        {
            vlmNr = vlmNrRef;
            extStorPoolName = extStorPoolNameRef;
        }

        public int getVlmNr()
        {
            return vlmNr;
        }

        public String getExtStorPoolName()
        {
            return extStorPoolName;
        }
    }
}
