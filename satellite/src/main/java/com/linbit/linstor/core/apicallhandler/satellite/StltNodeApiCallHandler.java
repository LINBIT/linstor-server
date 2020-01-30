package com.linbit.linstor.core.apicallhandler.satellite;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.DivergentUuidsException;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterfaceFactory;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.NodeConnectionFactory;
import com.linbit.linstor.core.objects.NodeSatelliteFactory;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

@Singleton
class StltNodeApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final DeviceManager deviceManager;
    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final CoreModule.NodesMap nodesMap;
    private final NodeSatelliteFactory nodeFactory;
    private final NodeConnectionFactory nodeConnectionFactory;
    private final NetInterfaceFactory netInterfaceFactory;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    StltNodeApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        DeviceManager deviceManagerRef,
        @Named(CoreModule.RECONFIGURATION_LOCK) ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        CoreModule.NodesMap nodesMapRef,
        NodeSatelliteFactory nodeFactoryRef,
        NodeConnectionFactory nodeConnectionFactoryRef,
        NetInterfaceFactory netInterfaceFactoryRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        deviceManager = deviceManagerRef;
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        nodesMap = nodesMapRef;
        nodeFactory = nodeFactoryRef;
        nodeConnectionFactory = nodeConnectionFactoryRef;
        netInterfaceFactory = netInterfaceFactoryRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        transMgrProvider = transMgrProviderRef;
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
            Set<ResourceName> rscToDeleteNames = new HashSet<>();
            if (removedNode != null)
            {
                List<Resource> rscToDelete = removedNode.streamResources(apiCtx).collect(Collectors.toList());
                for (Resource rsc : rscToDelete)
                {
                    rscToDeleteNames.add(rsc.getDefinition().getName());
                    rsc.delete(apiCtx);
                }
                removedNode.delete(apiCtx);
                transMgrProvider.get().commit();
            }

            errorReporter.logInfo("Node '" + nodeNameStr + "' removed by Controller.");

            Set<NodeName> updatedNodes = new TreeSet<>();
            updatedNodes.add(nodeName);
            deviceManager.nodeUpdateApplied(updatedNodes, rscToDeleteNames);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
    }

    public Node applyChanges(NodePojo nodePojo)
    {
        Lock reConfReadLock = reconfigurationLock.readLock();
        Lock nodesWriteLock = nodesMapLock.writeLock();

        Node node = null;
        try
        {
            reConfReadLock.lock();
            nodesWriteLock.lock();

            Node.Flags[] nodeFlags = Node.Flags.restoreFlags(nodePojo.getNodeFlags());
            node = nodeFactory.getInstanceSatellite(
                apiCtx,
                nodePojo.getUuid(),
                new NodeName(nodePojo.getName()),
                Node.Type.valueOf(nodePojo.getType()),
                nodeFlags
            );
            checkUuid(node, nodePojo);

            node.getFlags().resetFlagsTo(apiCtx, nodeFlags);

            Props nodeProps = node.getProps(apiCtx);
            nodeProps.map().putAll(nodePojo.getProps());
            nodeProps.keySet().retainAll(nodePojo.getProps().keySet());

            for (NodeConnPojo nodeConn : nodePojo.getNodeConns())
            {
                Node otherNode = nodeFactory.getInstanceSatellite(
                    apiCtx,
                    nodeConn.getOtherNodeUuid(),
                    new NodeName(nodeConn.getOtherNodeName()),
                    Node.Type.valueOf(nodeConn.getOtherNodeType()),
                    Node.Flags.restoreFlags(nodeConn.getOtherNodeFlags())
                );
                NodeConnection nodeCon = nodeConnectionFactory.getInstanceSatellite(
                    apiCtx,
                    nodeConn.getNodeConnUuid(),
                    node,
                    otherNode
                );
                Props nodeConnProps = nodeCon.getProps(apiCtx);
                nodeConnProps.map().putAll(nodeConn.getNodeConnProps());
                nodeConnProps.keySet().retainAll(nodeConn.getNodeConnProps().keySet());
            }

            for (NetInterfaceApi netIfApi : nodePojo.getNetInterfaces())
            {
                NetInterfaceName netIfName = new NetInterfaceName(netIfApi.getName());
                LsIpAddress ipAddress = new LsIpAddress(netIfApi.getAddress());
                NetInterface netIf = node.getNetInterface(apiCtx, netIfName);
                if (netIf == null)
                {
                    netInterfaceFactory.getInstanceSatellite(
                        apiCtx,
                        netIfApi.getUuid(),
                        node,
                        netIfName,
                        ipAddress
                    );
                }
                else
                {
                    netIf.setAddress(apiCtx, ipAddress);
                }
            }

            Set<ResourceName> rscSet = node.streamResources(apiCtx)
                .map(Resource::getDefinition)
                .map(ResourceDefinition::getName)
                .collect(Collectors.toSet());

            nodesMap.put(node.getName(), node);
            transMgrProvider.get().commit();

            errorReporter.logInfo("Node '" + nodePojo.getName() + "' created.");
            Set<NodeName> updatedNodes = new TreeSet<>();
            updatedNodes.add(new NodeName(nodePojo.getName()));
            deviceManager.nodeUpdateApplied(updatedNodes, rscSet);
        }
        catch (Exception | ImplementationError exc)
        {
            // TODO: kill connection?
            errorReporter.reportError(exc);
        }
        finally
        {
            nodesWriteLock.unlock();
            reConfReadLock.unlock();
        }
        return node;
    }

    private void checkUuid(Node node, NodePojo nodePojo) throws DivergentUuidsException
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
