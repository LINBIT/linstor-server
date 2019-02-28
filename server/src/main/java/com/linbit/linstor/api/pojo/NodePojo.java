package com.linbit.linstor.api.pojo;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.linbit.linstor.Node.NodeApi;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.netcom.Peer;

public class NodePojo implements NodeApi, Comparable<NodePojo>
{
    private final UUID nodeUuid;
    private final String nodeName;
    private final String nodeType;
    private final Peer.ConnectionStatus connectionStatus;
    private final long nodeFlags;
    private final List<NetInterface.NetInterfaceApi> nodeNetInterfaces;
    private final List<NodeConnPojo> nodeConns;
    private final Map<String, String> nodeProps;
    private long fullSyncId;
    private long updateId;

    public NodePojo(
        final UUID nodeUuidRef,
        final String nodeNameRef,
        final String nodeTypeRef,
        final long nodeFlagsRef,
        final List<NetInterface.NetInterfaceApi> nodeNetInterfacesRef,
        final List<NodeConnPojo> nodeConnsRef,
        final Map<String, String> nodePropsRef,
        final Peer.ConnectionStatus connectionStatusRef,
        long fullSyncIdRef,
        long updateIdRef
    )
    {
        nodeUuid = nodeUuidRef;
        nodeName = nodeNameRef;
        nodeType = nodeTypeRef;
        nodeFlags = nodeFlagsRef;
        nodeNetInterfaces = nodeNetInterfacesRef;
        nodeConns = nodeConnsRef;
        nodeProps = nodePropsRef;
        connectionStatus = connectionStatusRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
    }

    @Override
    public String getName()
    {
        return nodeName;
    }

    @Override
    public String getType()
    {
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
    public Peer.ConnectionStatus connectionStatus()
    {
        return connectionStatus;
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

    public long getFullSyncId()
    {
        return fullSyncId;
    }

    public long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(NodePojo otherNodePojo)
    {
        return nodeName.compareTo(otherNodePojo.nodeName);
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
