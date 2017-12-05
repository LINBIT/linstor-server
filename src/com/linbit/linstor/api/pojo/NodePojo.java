package com.linbit.linstor.api.pojo;

import com.linbit.linstor.Node;
import java.util.Map;
import java.util.UUID;

public class NodePojo implements Node.NodeApi
{
    private final UUID nodeUuid;
    private final String nodeName;
    private final String nodeType;
    private final long nodeFlags;
    private final Map<String, NetIfPojo> nodeNetInterfaces;
    private final Map<String, Map<String, String>> nodeConnProps;
    private final Map<String, String> nodeProps;

    public NodePojo(String name, UUID uuid, String type) {
        this.nodeName = name;
        this.nodeUuid = uuid;
        this.nodeType = type;
        this.nodeFlags = 0;
        this.nodeNetInterfaces = null;
        this.nodeConnProps = null;
        this.nodeProps = null;
    }

    public NodePojo(
        UUID nodeUuid,
        String nodeName,
        String nodeType,
        long nodeFlags,
        Map<String, NetIfPojo> nodeNetInterfaces,
        Map<String, Map<String, String>> nodeConnProps,
        Map<String, String> nodeProps
    )
    {
        this.nodeUuid = nodeUuid;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.nodeFlags = nodeFlags;
        this.nodeNetInterfaces = nodeNetInterfaces;
        this.nodeConnProps = nodeConnProps;
        this.nodeProps = nodeProps;
    }

    @Override
    public String getName()
    {
        return nodeName;
    }

    @Override
    public String getType() {
        return nodeType;
    }

    @Override
    public UUID getUuid()
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

    @Override
    public Map<String, String> getProps()
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
