package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RscPojo implements Comparable<RscPojo>, Resource.RscApi
{
    private final String rscName;
    private final String nodeName;
    private final UUID nodeUuid;
    private final ResourceDefinition.RscDfnApi rscDefinition;
    private final UUID localRscUuid;
    private final long localRscFlags;
    private final int localRscNodeId;
    private final Map<String, String> localRscProps;
    private final List<Volume.VlmApi> localVlms;
    private final List<OtherRscPojo> otherRscs;

    public RscPojo(
        String rscName,
        String nodeName,
        UUID nodeUuid,
        ResourceDefinition.RscDfnApi rscDefinition,
        UUID localRscUuid,
        long localRscFlags,
        int localRscNodeId,
        Map<String, String> localRscProps,
        List<Volume.VlmApi> localVlms,
        List<OtherRscPojo> otherRscList
    )
    {
        this.rscName = rscName;
        this.nodeName = nodeName;
        this.nodeUuid = nodeUuid;
        this.rscDefinition = rscDefinition;
        this.localRscUuid = localRscUuid;
        this.localRscFlags = localRscFlags;
        this.localRscNodeId = localRscNodeId;
        this.localRscProps = localRscProps;
        this.localVlms = localVlms;
        this.otherRscs = otherRscList;
    }

    @Override
    public String getName()
    {
        return rscName;
    }

    @Override
    public UUID getUuid()
    {
        return localRscUuid;
    }

    @Override
    public String getNodeName()
    {
        return nodeName;
    }

    @Override
    public UUID getNodeUuid()
    {
        return nodeUuid;
    }

    @Override
    public UUID getRscDfnUuid()
    {
        return rscDefinition.getUuid();
    }

    public int getRscDfnPort()
    {
        return rscDefinition.getPort();
    }

    public long getRscDfnFlags()
    {
        return rscDefinition.getFlags();
    }

    public String getRscDfnSecret()
    {
        return rscDefinition.getSecret();
    }

    public Map<String, String> getRscDfnProps()
    {
        return rscDefinition.getProps();
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

    public List<VolumeDefinition.VlmDfnApi> getVlmDfns()
    {
        return rscDefinition.getVlmDfnList();
    }

    public List<Volume.VlmApi> getLocalVlms()
    {
        return localVlms;
    }

    @Override
    public Map<String, String> getProps() {
        return localRscProps;
    }

    @Override
    public List<? extends Volume.VlmApi> getVlmList() {
        return localVlms;
    }

    public List<OtherRscPojo> getOtherRscList()
    {
        return otherRscs;
    }

    @Override
    public int compareTo(RscPojo otherRscPojo)
    {
        return rscName.compareTo(otherRscPojo.rscName);
    }

    public static class OtherRscPojo
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
        private final List<Volume.VlmApi> vlms;

        public OtherRscPojo(
            String nodeName,
            UUID nodeUuid,
            long nodeType,
            long nodeFlags,
            Map<String, String> nodeProps,
            UUID rscUuid,
            int rscNodeId,
            long rscFlags,
            Map<String, String> rscProps,
            List<Volume.VlmApi> vlms
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

        public List<Volume.VlmApi> getVlms()
        {
            return vlms;
        }
    }
}
