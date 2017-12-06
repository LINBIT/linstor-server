package com.linbit.linstor.api.pojo;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.Node.NodeApi;

public class NodePojo implements NodeApi, Comparable<NodePojo>
{
    private final UUID nodeUuid;
    private final String nodeName;
    private final String nodeType;
    private final long nodeFlags;
    private final List<NetIfPojo> nodeNetInterfaces;
    private final List<NodeConnPojo> nodeConns;
    private final Map<String, String> nodeProps;

    public NodePojo(String name, UUID uuid, String type) {
        this.nodeName = name;
        this.nodeUuid = uuid;
        this.nodeType = type;
        this.nodeFlags = 0;
        this.nodeNetInterfaces = null;
        this.nodeConns = null;
        this.nodeProps = null;
    }

    public NodePojo(
        UUID nodeUuid,
        String nodeName,
        String nodeType,
        long nodeFlags,
        List<NetIfPojo> nodeNetInterfaces,
        List<NodeConnPojo> nodeConns,
        Map<String, String> nodeProps
    )
    {
        this.nodeUuid = nodeUuid;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.nodeFlags = nodeFlags;
        this.nodeNetInterfaces = nodeNetInterfaces;
        this.nodeConns = nodeConns;
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

    public List<NetIfPojo> getNodeNetInterfaces()
    {
        return nodeNetInterfaces;
    }

    public List<NodeConnPojo> getNodeConns()
    {
        return nodeConns;
    }

    @Override
    public Map<String, String> getProps()
    {
        return nodeProps;
    }

    @Override
    public int compareTo(NodePojo otherNodePojo)
    {
        return nodeName.compareTo(otherNodePojo.nodeName);
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

    public static class NodeConnPojo
    {
        private final UUID nodeConnUuid;
        private final UUID otherNodeUuid;
        private final String otherNodeName;
        private final String otherNodeType;
        private final long otherNodeFlags;
        private final Map<String, String> nodeConnProps;

        public NodeConnPojo(
            UUID nodeConnUuidRef,
            UUID otherNodeUuidRef,
            String otherNodeNameRef,
            String otherNodeTypeRef,
            long otherNodeFlagsRef,
            Map<String, String> nodeConnPropsRef
        )
        {
            nodeConnUuid = nodeConnUuidRef;
            otherNodeUuid = otherNodeUuidRef;
            otherNodeName = otherNodeNameRef;
            otherNodeType = otherNodeTypeRef;
            otherNodeFlags = otherNodeFlagsRef;
            nodeConnProps = nodeConnPropsRef;
        }

        public UUID getNodeConnUuid()
        {
            return nodeConnUuid;
        }

        public UUID getOtherNodeUuid()
        {
            return otherNodeUuid;
        }

        public String getOtherNodeName()
        {
            return otherNodeName;
        }

        public String getOtherNodeType()
        {
            return otherNodeType;
        }

        public long getOtherNodeFlags()
        {
            return otherNodeFlags;
        }

        public Map<String, String> getNodeConnProps()
        {
            return nodeConnProps;
        }
    }
}
