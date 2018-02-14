package com.linbit.linstor.core;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.linstor.LsIpAddress;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.NetInterfaceData;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeFlag;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeConnectionData;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
class StltNodeApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final CoreModule.NodesMap nodesMap;

    @Inject
    StltNodeApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CoreModule.NodesMap nodesMapRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        nodesMap = nodesMapRef;
    }

    /**
     * We requested an update to a node and the controller is telling us that the requested node
     * does no longer exist.
     * Basically we now just mark the update as received and applied to prevent the
     * {@link DeviceManager} from waiting for that update.
     *
     * @param nodeNameStr
     */
    public void applyDeletedNode(String nodeNameStr)
    {
        try
        {
            NodeName nodeName = new NodeName(nodeNameStr);

            Node removedNode = nodesMap.remove(nodeName); // just to be sure
            if (removedNode != null)
            {
                SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
                removedNode.setConnection(transMgr);
                removedNode.delete(apiCtx);
                transMgr.commit();
            }

            errorReporter.logInfo("Node '" + nodeNameStr + "' removed by Controller.");

            Set<NodeName> updatedNodes = new TreeSet<>();
            updatedNodes.add(nodeName);
            deviceManager.nodeUpdateApplied(updatedNodes);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    public void applyChanges(NodePojo nodePojo)
    {
        try
        {
            SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();

            Node node = applyChanges(nodePojo, transMgr);
            NodeName nodeName = new NodeName(nodePojo.getName());
            transMgr.commit();

            nodesMap.put(nodeName, node);
            errorReporter.logInfo("Node '" + nodePojo.getName() + "' created.");
            Set<NodeName> updatedNodes = new TreeSet<>();
            updatedNodes.add(new NodeName(nodePojo.getName()));
            deviceManager.nodeUpdateApplied(updatedNodes);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    public NodeData applyChanges(NodePojo nodePojo, SatelliteTransactionMgr transMgr)
        throws DivergentUuidsException, ImplementationError, InvalidNameException, AccessDeniedException,
        SQLException, InvalidIpAddressException
    {
        Lock reConfReadLock = reconfigurationLock.readLock();
        Lock nodesWriteLock = nodesMapLock.writeLock();

        NodeData node;
        try
        {
            reConfReadLock.lock();
            nodesWriteLock.lock();

            NodeFlag[] nodeFlags = NodeFlag.restoreFlags(nodePojo.getNodeFlags());
            node = NodeData.getInstanceSatellite(
                apiCtx,
                nodePojo.getUuid(),
                new NodeName(nodePojo.getName()),
                NodeType.valueOf(nodePojo.getType()),
                nodeFlags,
                nodePojo.getDisklessStorPoolUuid(),
                transMgr
            );
            checkUuid(node, nodePojo);

            node.getFlags().resetFlagsTo(apiCtx, nodeFlags);

            Map<String, String> map = node.getProps(apiCtx).map();
            map.clear();
            map.putAll(nodePojo.getProps());

            for (NodeConnPojo nodeConn : nodePojo.getNodeConns())
            {
                NodeData otherNode = NodeData.getInstanceSatellite(
                    apiCtx,
                    nodeConn.getOtherNodeUuid(),
                    new NodeName(nodeConn.getOtherNodeName()),
                    NodeType.valueOf(nodeConn.getOtherNodeType()),
                    NodeFlag.restoreFlags(nodeConn.getOtherNodeFlags()),
                    nodeConn.getOtherNodeDisklessStorPoolUuid(),
                    transMgr
                );
                NodeConnectionData nodeCon = NodeConnectionData.getInstanceSatellite(
                    apiCtx,
                    nodeConn.getNodeConnUuid(),
                    node,
                    otherNode,
                    transMgr
                );
                Map<String, String> props = nodeCon.getProps(apiCtx).map();
                props.clear();
                props.putAll(nodeConn.getNodeConnProps());
            }

            for (NetInterface.NetInterfaceApi netIfApi : nodePojo.getNetInterfaces())
            {
                NetInterfaceName netIfName = new NetInterfaceName(netIfApi.getName());
                LsIpAddress ipAddress = new LsIpAddress(netIfApi.getAddress());
                NetInterface netIf = node.getNetInterface(apiCtx, netIfName);
                if (netIf == null)
                {
                    NetInterfaceData.getInstanceSatellite(
                        apiCtx,
                        netIfApi.getUuid(),
                        node,
                        netIfName,
                        ipAddress,
                        transMgr
                    );
                }
                else
                {
                    netIf.setAddress(apiCtx, ipAddress);
                }
            }
        }
        finally
        {
            nodesWriteLock.unlock();
            reConfReadLock.unlock();
        }
        return node;
    }

    private void checkUuid(NodeData node, NodePojo nodePojo) throws DivergentUuidsException
    {
        checkUuid(
            node.getUuid(),
            nodePojo.getUuid(),
            "Node",
            node.getName().displayValue,
            nodePojo.getName()
        );
    }

    private void checkUuid(UUID localUuid, UUID remoteUuid, String type, String localName, String remoteName)
        throws DivergentUuidsException
    {
        if (!localUuid.equals(remoteUuid))
        {
            throw new DivergentUuidsException(
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }
}
