package com.linbit.drbdmanage.api.raw;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ResourceRawData
{
    private final String rscName;
    private final UUID rscDfnUuid;
    private final int rscDfnPort;
    private final long rscDfnFlags;
    private final String rscDfnSecret;
    private final Map<String, String> rscDfnProps;
    private final UUID localRscUuid;
    private final long localRscFlags;
    private final int localRscNodeId;
    private final Map<String, String> localRscProps;
    private final List<VolumeDfnRawData> vlmDfns;
    private final List<VolumeRawData> localVlms;
    private final List<OtherRscRawData> otherRscs;

    public ResourceRawData(
        String rscName,
        UUID rscDfnUuid,
        int port,
        long rscDfnFlags,
        String rscDfnSecret,
        Map<String, String> rscDfnProps,
        UUID localRscUuid,
        long localRscFlags,
        int localRscNodeId,
        Map<String, String> localRscProps,
        List<VolumeDfnRawData> vlmDfns,
        List<VolumeRawData> localVlms,
        List<OtherRscRawData> otherRscList
    )
    {
        this.rscName = rscName;
        this.rscDfnUuid = rscDfnUuid;
        this.rscDfnPort = port;
        this.rscDfnFlags = rscDfnFlags;
        this.rscDfnSecret = rscDfnSecret;
        this.rscDfnProps = rscDfnProps;
        this.localRscUuid = localRscUuid;
        this.localRscFlags = localRscFlags;
        this.localRscNodeId = localRscNodeId;
        this.localRscProps = localRscProps;
        this.vlmDfns = vlmDfns;
        this.localVlms = localVlms;
        this.otherRscs = otherRscList;
    }

    public String getRscName()
    {
        return rscName;
    }

    public UUID getRscDfnUuid()
    {
        return rscDfnUuid;
    }

    public int getRscDfnPort()
    {
        return rscDfnPort;
    }

    public long getRscDfnFlags()
    {
        return rscDfnFlags;
    }

    public String getRscDfnSecret()
    {
        return rscDfnSecret;
    }

    public Map<String, String> getRscDfnProps()
    {
        return rscDfnProps;
    }

    public UUID getLocalRscUuid()
    {
        return localRscUuid;
    }

    public long getLocalRscFlags()
    {
        return localRscFlags;
    }

    public int getLocalRscNodeId()
    {
        return localRscNodeId;
    }

    public Map<String, String> getLocalRscProps()
    {
        return localRscProps;
    }

    public List<VolumeDfnRawData> getVlmDfns()
    {
        return vlmDfns;
    }

    public List<VolumeRawData> getLocalVlms()
    {
        return localVlms;
    }

    public List<OtherRscRawData> getOtherRscList()
    {
        return otherRscs;
    }

    public static class VolumeDfnRawData
    {
        private final UUID vlmDfnUuid;
        private final int vlmNr;
        private final long vlmSize;
        private final int vlmMinor;
        private final long flags;
        private final Map<String, String> vlmDfnProps;

        public VolumeDfnRawData(
            UUID uuid,
            int vlmNr,
            long vlmSize,
            int vlmMinor,
            long flags,
            Map<String, String> vlmDfnProps
        )
        {
            this.vlmDfnUuid = uuid;
            this.vlmNr = vlmNr;
            this.vlmSize = vlmSize;
            this.vlmMinor = vlmMinor;
            this.flags = flags;
            this.vlmDfnProps = vlmDfnProps;
        }

        public UUID getVlmDfnUuid()
        {
            return vlmDfnUuid;
        }

        public int getVlmNr()
        {
            return vlmNr;
        }

        public long getVlmSize()
        {
            return vlmSize;
        }

        public int getVlmMinor()
        {
            return vlmMinor;
        }

        public long getFlags()
        {
            return flags;
        }

        public Map<String, String> getVlmDfnProps()
        {
            return vlmDfnProps;
        }
    }

    public static class VolumeRawData
    {
        private final UUID vlmUuid;
        private final int vlmNr;
        private final String blockDevice;
        private final String metaDisk;
        private final long vlmFlags;
        private final UUID storPoolUuid;
        private final String storPoolName;
        private final Map<String, String> props;

        public VolumeRawData(
            UUID vlmUuid,
            int vlmNr,
            String blockDevice,
            String metaDisk,
            long vlmFlags,
            UUID storPoolUuid,
            String storPoolName,
            Map<String, String> props
        )
        {
            this.vlmUuid = vlmUuid;
            this.vlmNr = vlmNr;
            this.blockDevice = blockDevice;
            this.metaDisk = metaDisk;
            this.vlmFlags = vlmFlags;
            this.storPoolUuid = storPoolUuid;
            this.storPoolName = storPoolName;
            this.props = props;
        }

        public UUID getVlmUuid()
        {
            return vlmUuid;
        }

        public int getVlmNr()
        {
            return vlmNr;
        }

        public String getBlockDevice()
        {
            return blockDevice;
        }

        public String getMetaDisk()
        {
            return metaDisk;
        }

        public long getVlmFlags()
        {
            return vlmFlags;
        }

        public UUID getStorPoolUuid()
        {
            return storPoolUuid;
        }

        public String getStorPoolName()
        {
            return storPoolName;
        }

        public Map<String, String> getProps()
        {
            return props;
        }
    }

    public static class OtherRscRawData
    {

        private final String nodeName;
        private final UUID nodeUuid;
        private final long nodeType;
        private final long nodeFlags;
        private final Map<String, String> nodeProps;
        private final UUID rscUuid;
        private final int rscNodeId;
        private final long rscFlags;
        private final Map<String, String> rscProps;
        private final List<VolumeRawData> vlms;

        public OtherRscRawData(
            String nodeName,
            UUID nodeUuid,
            long nodeType,
            long nodeFlags,
            Map<String, String> nodeProps,
            UUID rscUuid,
            int rscNodeId,
            long rscFlags,
            Map<String, String> rscProps,
            List<VolumeRawData> vlms
        )
        {
            this.nodeName = nodeName;
            this.nodeUuid = nodeUuid;
            this.nodeType = nodeType;
            this.nodeFlags = nodeFlags;
            this.nodeProps = nodeProps;
            this.rscUuid = rscUuid;
            this.rscNodeId = rscNodeId;
            this.rscFlags = rscFlags;
            this.rscProps = rscProps;
            this.vlms = vlms;
        }

        public String getNodeName()
        {
            return nodeName;
        }

        public UUID getNodeUuid()
        {
            return nodeUuid;
        }

        public long getNodeType()
        {
            return nodeType;
        }

        public long getNodeFlags()
        {
            return nodeFlags;
        }

        public Map<String, String> getNodeProps()
        {
            return nodeProps;
        }

        public UUID getRscUuid()
        {
            return rscUuid;
        }

        public int getRscNodeId()
        {
            return rscNodeId;
        }

        public long getRscFlags()
        {
            return rscFlags;
        }

        public Map<String, String> getRscProps()
        {
            return rscProps;
        }

        public List<VolumeRawData> getVlms()
        {
            return vlms;
        }
    }
}
