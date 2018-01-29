package com.linbit.linstor.api.pojo;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.Node.NodeApi;
import com.linbit.linstor.NetInterface;

public class NodePojo implements NodeApi, Comparable<NodePojo>
{
    private final UUID nodeUuid;
    private final String nodeName;
    private final String nodeType;
    private final Boolean isConnected;
    private final long nodeFlags;
    private final List<NetInterface.NetInterfaceApi> nodeNetInterfaces;
    private final List<NodeConnPojo> nodeConns;
    private final Map<String, String> nodeProps;
    private final UUID disklessStorPoolUuid;
    private final Long fullSyncId;
    private final Long updateId;

    public NodePojo(
        final UUID nodeUuid,
        final String nodeName,
        final String nodeType,
        final long nodeFlags,
        final List<NetInterface.NetInterfaceApi> nodeNetInterfaces,
        final List<NodeConnPojo> nodeConns,
        final Map<String, String> nodeProps,
        final boolean connected,
        final UUID disklessStorPoolUuid,
        final Long fullSyncId,
        final Long updateId
    )
    {
        this.nodeUuid = nodeUuid;
        this.nodeName = nodeName;
        this.nodeType = nodeType;
        this.nodeFlags = nodeFlags;
        this.nodeNetInterfaces = nodeNetInterfaces;
        this.nodeConns = nodeConns;
        this.nodeProps = nodeProps;
        this.disklessStorPoolUuid = disklessStorPoolUuid;
        this.isConnected = connected;
        this.fullSyncId = fullSyncId;
        this.updateId = updateId;
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

    @Override
    public Boolean isConnected()
    {
        return isConnected;
    }

    @Override
    public List<NetInterface.NetInterfaceApi> getNetInterfaces()
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
    public long getFlags()
    {
        return nodeFlags;
    }

    @Override
    public int compareTo(NodePojo otherNodePojo)
    {
        return nodeName.compareTo(otherNodePojo.nodeName);
    }

    @Override
    public UUID getDisklessStorPoolUuid()
    {
        return disklessStorPoolUuid;
    }

    public long getFullSyncId()
    {
        return fullSyncId;
    }

    public long getUpdateId()
    {
        return updateId;
    }

    public static class NodeConnPojo
    {
        private final UUID nodeConnUuid;
        private final UUID otherNodeUuid;
        private final String otherNodeName;
        private final String otherNodeType;
        private final long otherNodeFlags;
        private final Map<String, String> nodeConnProps;
        private final UUID otherNodeDisklessStorPoolUuid;

        public NodeConnPojo(
            UUID nodeConnUuidRef,
            UUID otherNodeUuidRef,
            String otherNodeNameRef,
            String otherNodeTypeRef,
            long otherNodeFlagsRef,
            Map<String, String> nodeConnPropsRef,
            UUID otherNodeDisklessStorPoolUuidRef
        )
        {
            nodeConnUuid = nodeConnUuidRef;
            otherNodeUuid = otherNodeUuidRef;
            otherNodeName = otherNodeNameRef;
            otherNodeType = otherNodeTypeRef;
            otherNodeFlags = otherNodeFlagsRef;
            nodeConnProps = nodeConnPropsRef;
            otherNodeDisklessStorPoolUuid = otherNodeDisklessStorPoolUuidRef;
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

        public UUID getOtherNodeDisklessStorPoolUuid()
        {
            return otherNodeDisklessStorPoolUuid;
        }
    }
}
