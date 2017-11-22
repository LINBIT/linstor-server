package com.linbit.linstor.api.pojo;

import java.util.Map;
import java.util.UUID;

public class NodePojo
{
    private final UUID nodeUuid;
    private final String nodeName;
    private final long nodeFlags;
    private final Map<String, NetIfPojo> nodeNetInterfaces;
    private final Map<String, Map<String, String>> nodeConnProps;
    private final Map<String, String> nodeProps;

    public NodePojo(
        UUID nodeUuid,
        String nodeName,
        long nodeFlags,
        Map<String, NetIfPojo> nodeNetInterfaces,
        Map<String, Map<String, String>> nodeConnProps,
        Map<String, String> nodeProps
    )
    {
        this.nodeUuid = nodeUuid;
        this.nodeName = nodeName;
        this.nodeFlags = nodeFlags;
        this.nodeNetInterfaces = nodeNetInterfaces;
        this.nodeConnProps = nodeConnProps;
        this.nodeProps = nodeProps;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public UUID getNodeUuid()
    {
        return nodeUuid;
    }

    public long getNodeFlags()
    {
        return nodeFlags;
    }

    public Map<String, NetIfPojo> getNodeNetInterfaces()
    {
        return nodeNetInterfaces;
    }

    public Map<String, Map<String, String>> getNodeConnProps()
    {
        return nodeConnProps;
    }

    public Map<String, String> getNodeProps()
    {
        return nodeProps;
    }

    public static class NetIfPojo
    {
        private final UUID netIfUuid;
        private final String netIfName;
        private final String netIfAddr;
        private final String netIfType;
        private final int netIfPort;

        public NetIfPojo(
            UUID netIfUuid,
            String netIfName,
            String netIfAddr,
            String netIfType,
            int netIfPort
        )
        {
            this.netIfUuid = netIfUuid;
            this.netIfName = netIfName;
            this.netIfAddr = netIfAddr;
            this.netIfType = netIfType;
            this.netIfPort = netIfPort;
        }

        public UUID getNetIfUuid()
        {
            return netIfUuid;
        }

        public String getNetIfName()
        {
            return netIfName;
        }

        public String getNetIfAddr()
        {
            return netIfAddr;
        }

        public String getNetIfType()
        {
            return netIfType;
        }

        public int getNetIfPort()
        {
            return netIfPort;
        }
    }
}
