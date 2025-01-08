package com.linbit.linstor.api.pojo;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.apis.NodeApi;
import com.linbit.linstor.core.apis.NodeConnectionApi;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class NodePojo implements NodeApi, Comparable<NodePojo>
{
    private final UUID nodeUuid;
    private final String nodeName;
    private final String nodeType;
    private final ApiConsts.ConnectionStatus connectionStatus;
    private final long nodeFlags;
    private final List<NetInterfaceApi> nodeNetInterfaces;
    private final NetInterfaceApi nodeActiveStltConn;
    private final List<NodeConnPojo> nodeConns;
    private final Map<String, String> nodeProps;
    private final Long fullSyncId;
    private final Long updateId;
    private final List<String> deviceLayerKindNames;
    private final List<String> deviceProviderKindNames;
    private final Map<String, List<String>> unsupportedLayersWithReasons;
    private final Map<String, List<String>> unsupportedProvidersWithReasons;
    private final Long evictionTimestamp;
    private final long reconnectAttemptCount;

    public NodePojo(
        final UUID nodeUuidRef,
        final String nodeNameRef,
        final String nodeTypeRef,
        final long nodeFlagsRef,
        final List<NetInterfaceApi> nodeNetInterfacesRef,
        @Nullable final NetInterfaceApi nodeActiveStltConnRef,
        final List<NodeConnPojo> nodeConnsRef,
        final Map<String, String> nodePropsRef,
        final ApiConsts.ConnectionStatus connectionStatusRef,
        final Long fullSyncIdRef,
        final Long updateIdRef,
        final List<String> deviceLayerKindNamesRef,
        final List<String> deviceProviderKindNamesRef,
        final Map<String, List<String>> unsupportedLayersWithReasonsRef,
        final Map<String, List<String>> unsupportedProvidersWithReasonsRef,
        final Long evictionTimestampRef,
        final long reconnectAttemptCountRef
    )
    {
        nodeUuid = nodeUuidRef;
        nodeName = nodeNameRef;
        nodeType = nodeTypeRef;
        nodeFlags = nodeFlagsRef;
        nodeNetInterfaces = nodeNetInterfacesRef;
        nodeActiveStltConn = nodeActiveStltConnRef;
        nodeConns = nodeConnsRef;
        nodeProps = nodePropsRef;
        connectionStatus = connectionStatusRef;
        fullSyncId = fullSyncIdRef;
        updateId = updateIdRef;
        deviceLayerKindNames = deviceLayerKindNamesRef;
        deviceProviderKindNames = deviceProviderKindNamesRef;
        unsupportedLayersWithReasons = unsupportedLayersWithReasonsRef;
        unsupportedProvidersWithReasons = unsupportedProvidersWithReasonsRef;
        evictionTimestamp = evictionTimestampRef;
        reconnectAttemptCount = reconnectAttemptCountRef;
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
    public ApiConsts.ConnectionStatus connectionStatus()
    {
        return connectionStatus;
    }

    @Override
    public long getReconnectAttemptCount()
    {
        return reconnectAttemptCount;
    }

    @Override
    public List<NetInterfaceApi> getNetInterfaces()
    {
        return nodeNetInterfaces;
    }

    @Override
    public NetInterfaceApi getActiveStltConn()
    {
        return nodeActiveStltConn;
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

    public long getFullSyncId()
    {
        return fullSyncId;
    }

    public long getUpdateId()
    {
        return updateId;
    }

    @Override
    public List<String> getDeviceLayerKindNames()
    {
        return deviceLayerKindNames;
    }

    @Override
    public List<String> getDeviceProviderKindNames()
    {
        return deviceProviderKindNames;
    }

    @Override
    public Map<String, List<String>> getUnsupportedLayersWithReasons()
    {
        return unsupportedLayersWithReasons;
    }

    @Override
    public Map<String, List<String>> getUnsupportedProvidersWithReasons()
    {
        return unsupportedProvidersWithReasons;
    }

    @Override
    public Long getEvictionTimestamp()
    {
        return evictionTimestamp;
    }

    public static class NodeConnPojo implements NodeConnectionApi, Comparable<NodeConnPojo>
    {
        private final UUID uuid;
        private final String localNodeName;
        private final NodePojo otherNodePojo;

        private final Map<String, String> props;

        public NodeConnPojo(
            UUID nodeConnUuidRef,
            String localNodeNameRef,
            NodePojo otherNodePojoRef,
            Map<String, String> nodeConnPropsRef
        )
        {
            uuid = nodeConnUuidRef;
            localNodeName = localNodeNameRef;
            otherNodePojo = otherNodePojoRef;
            props = nodeConnPropsRef;
        }

        @Override
        public UUID getUuid()
        {
            return uuid;
        }

        @Override
        public String getLocalNodeName()
        {
            return localNodeName;
        }

        @Override
        public NodePojo getOtherNodeApi()
        {
            return otherNodePojo;
        }

        @Override
        public Map<String, String> getProps()
        {
            return props;
        }

        @Override
        public int compareTo(NodeConnPojo otherRef)
        {
            int cmp = localNodeName.compareTo(otherRef.localNodeName);
            if (cmp == 0)
            {
                cmp = otherNodePojo.nodeName.compareTo(otherRef.otherNodePojo.nodeName);
            }
            return cmp;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(uuid);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof NodeConnPojo))
            {
                return false;
            }
            NodeConnPojo other = (NodeConnPojo) obj;
            return Objects.equals(uuid, other.uuid);
        }
    }
}
