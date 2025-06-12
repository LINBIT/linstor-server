package com.linbit.linstor.core.apicallhandler;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.core.StltConfigAccessor;
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
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

@Singleton
public class StltNodeApiCallHandler
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
    private final Provider<TransactionMgr> transMgrProvider;
    private final ControllerPeerConnector controllerPeerConnector;
    private final StltConfigAccessor stltConfigAccessor;

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
        StltConfigAccessor stltConfigAccessorRef,
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
        stltConfigAccessor = stltConfigAccessorRef;
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
                    rscToDeleteNames.add(rsc.getResourceDefinition().getName());
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

    public @Nullable Node applyChanges(NodePojo nodePojo)
    {
        Lock reConfReadLock = reconfigurationLock.readLock();
        Lock nodesWriteLock = nodesMapLock.writeLock();

        Node curNode = null;
        try
        {
            reConfReadLock.lock();
            nodesWriteLock.lock();

            Node.Flags[] nodeFlags = Node.Flags.restoreFlags(nodePojo.getNodeFlags());
            curNode = nodeFactory.getInstanceSatellite(
                apiCtx,
                nodePojo.getUuid(),
                new NodeName(nodePojo.getName()),
                Node.Type.valueOf(nodePojo.getType()),
                nodeFlags
            );
            checkUuid(curNode, nodePojo);

            curNode.getFlags().resetFlagsTo(apiCtx, nodeFlags);

            Props nodeProps = curNode.getProps(apiCtx);
            nodeProps.map().putAll(nodePojo.getProps());
            nodeProps.keySet().retainAll(nodePojo.getProps().keySet());

            mergeNodeConnections(curNode, nodePojo);

            for (NetInterfaceApi netIfApi : nodePojo.getNetInterfaces())
            {
                NetInterfaceName netIfName = new NetInterfaceName(netIfApi.getName());
                LsIpAddress ipAddress = new LsIpAddress(netIfApi.getAddress());
                NetInterface netIf = curNode.getNetInterface(apiCtx, netIfName);
                if (netIf == null)
                {
                    netInterfaceFactory.getInstanceSatellite(
                        apiCtx,
                        netIfApi.getUuid(),
                        curNode,
                        netIfName,
                        ipAddress
                    );
                }
                else
                {
                    netIf.setAddress(apiCtx, ipAddress);
                }
            }

            Set<ResourceName> rscSet = curNode.streamResources(apiCtx)
                .map(Resource::getResourceDefinition)
                .map(ResourceDefinition::getName)
                .collect(Collectors.toSet());

            transMgrProvider.get().commit();

            LvmUtils.updateCacheTime(stltConfigAccessor.getReadonlyProps(), nodeProps);

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
        return curNode;
    }

    private void mergeNodeConnections(Node localNode, NodePojo nodePojo)
        throws AccessDeniedException, ImplementationError, InvalidNameException, DatabaseException
    {
        // do not compare getLocalNode().equals(localNode), since .getLocalNodeName() queries the nodesMap
        // which was cleared at the beginning of the FullSync. If we are alphanumerically later than the
        // other node, this check would run a "null.equals(localNode)" -> NPE
        if (controllerPeerConnector.getLocalNodeName().equals(localNode.getName()))
        {
            /*
             * The controller only sends our nodeConnections and that also only in our nodePojo.
             * That means, if we are node A, only nodeA's pojo will contain node connections to
             * i.e. nodeB. nodeB's pojo will *not* contain any node connections, not even to
             * nodeA (to prevent recursion).
             *
             * Therefore we only have to merge nodeConnections if curNode == localNode.
             *
             * If we would still call this method for other nodes, since those node's pojos do not
             * contain node connections to our local node, otherNode.removeNodeConnection will be
             * called. If we then try to update that node connection (i.e. by setting a property),
             * only our local node will have a reference to the node connection, but not the other
             * node, which will result in an error.
             */

            List<NodeConnection> nodeConsToDelete = new ArrayList<>(localNode.getNodeConnections(apiCtx));
            for (NodeConnPojo nodeConn : nodePojo.getNodeConns())
            {
                NodePojo otherNodePojo = nodeConn.getOtherNodeApi();
                Node otherNode = nodeFactory.getInstanceSatellite(
                    apiCtx,
                    otherNodePojo.getUuid(),
                    new NodeName(otherNodePojo.getName()),
                    Node.Type.valueOf(otherNodePojo.getType()),
                    Node.Flags.restoreFlags(otherNodePojo.getFlags())
                );
                NodeConnection nodeCon = nodeConnectionFactory.getInstanceSatellite(
                    apiCtx,
                    nodeConn.getUuid(),
                    localNode,
                    otherNode
                );
                nodeConsToDelete.remove(nodeCon);
                Props nodeConnProps = nodeCon.getProps(apiCtx);
                nodeConnProps.map().putAll(nodeConn.getProps());
                nodeConnProps.keySet().retainAll(nodeConn.getProps().keySet());
            }
            for (NodeConnection nodeConToDelete : nodeConsToDelete)
            {
                Node otherNode = nodeConToDelete.getOtherNode(apiCtx, localNode);
                localNode.removeNodeConnection(apiCtx, nodeConToDelete);
                if (!deleteRemoteNodeIfNeeded(apiCtx, nodesMap, localNode, otherNode))
                {
                    otherNode.removeNodeConnection(apiCtx, nodeConToDelete);
                }
            }
        }
    }

    public static boolean canRemoteNodeBeDeleted(
        AccessContext apiCtxRef,
        Node localNodeRef,
        Node remoteNodeRef
    )
        throws AccessDeniedException
    {
        /*
         * Bugfix: if the remoteRsc was the last resource of the remote node
         * we will no longer receive updates about the remote node (why should we?)
         * The problem is, that if the remote node gets completely deleted
         * on the controller, and later recreated, and that "new" node deploys
         * a resource we are also interested in, we will receive the "new" node's UUID.
         * However, we will still find our old node-reference when looking up the
         * "new" node's name and therefore we will find the old node's UUID and check it
         * against the "new" node's UUID.
         * This will cause a UUID mismatch upon resource-creation on the other node
         * (which will trigger an update to us as we also need to know about the new resource
         * and it's node)
         *
         * Therefore, we have to remove the remoteNode completely if it has no
         * resources left
         */
        /*
         * Exception of the rule above is if there still exist a nodeConnection to that remote node
         */

        return remoteNodeRef.getResourceCount() < 1 &&
            localNodeRef.getNodeConnection(apiCtxRef, remoteNodeRef) == null;
    }

    public static boolean deleteRemoteNodeIfNeeded(
        AccessContext apiCtxRef,
        NodesMap nodesMapRef,
        Node localNodeRef,
        Node remoteNodeRef
    )
        throws AccessDeniedException, DatabaseException
    {

        boolean deleted = false;
        boolean shouldDelete = canRemoteNodeBeDeleted(apiCtxRef, localNodeRef, remoteNodeRef);
        if (shouldDelete)
        {
            nodesMapRef.remove(remoteNodeRef.getName());
            remoteNodeRef.delete(apiCtxRef);
            deleted = true;
        }

        return deleted;
    }

    private void checkUuid(Node node, NodePojo nodePojo)
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
    {
        if (!localUuid.equals(remoteUuid))
        {
            CriticalError.dieUuidMissmatch(
                errorReporter,
                type,
                localName,
                remoteName,
                localUuid,
                remoteUuid
            );
        }
    }
}
