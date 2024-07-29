package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Defines a connection between two LinStor nodes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NodeConnection extends AbsCoreObj<NodeConnection>
{
    private final Node sourceNode;
    private final Node targetNode;

    private final Props props;

    private final NodeConnectionDatabaseDriver dbDriver;

    private final Key nodeConnKey;

    /**
     * Use NodeConnection.createWithSorting instead
     */
    private NodeConnection(
        UUID uuid,
        Node node1,
        Node node2,
        NodeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
        throws DatabaseException
    {
        super(uuid, transObjFactory, transMgrProviderRef);

        dbDriver = dbDriverRef;
        sourceNode = node1;
        targetNode = node2;
        nodeConnKey = new Key(this);

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(
                sourceNode.getName(),
                targetNode.getName()
            ),
            toStringImpl(),
            LinStorObject.NODE_CONN
        );

        transObjs = Arrays.asList(
            sourceNode,
            targetNode,
            props,
            deleted
        );
    }

    public static NodeConnection createWithSorting(
        UUID uuid,
        Node node1,
        Node node2,
        NodeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        AccessContext accCtx
    ) throws LinStorDataAlreadyExistsException, AccessDeniedException, DatabaseException
    {
        node1.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        node2.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        NodeConnection node1ConData = node1.getNodeConnection(accCtx, node2);
        NodeConnection node2ConData = node2.getNodeConnection(accCtx, node1);

        if (node1ConData != null || node2ConData != null)
        {
            if (node1ConData != null && node2ConData != null)
            {
                throw new LinStorDataAlreadyExistsException("The NodeConnection already exists");
            }
            throw new LinStorDataAlreadyExistsException(
                "The NodeConnection already exists for one of the nodes"
            );
        }

        // if this is changed, please also update Json#apiToNodeConnection method
        Node src;
        Node dst;
        int comp = node1.getName().compareTo(node2.getName());
        if (comp < 0)
        {
            src = node1;
            dst = node2;
        }
        else if (comp > 0)
        {
            src = node2;
            dst = node1;
        }
        else
        {
            throw new ImplementationError("Cannot create a node connection to the same node");
        }

        return createForDb(
            uuid,
            src,
            dst,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProviderRef
        );
    }

    /**
     * WARNING: do not use this method unless you are absolutely sure the resourceConnection you are trying to create
     * does not exist yet and the resources are already sorted correctly.
     * If you are not sure they are, use NodeConnection.createWithSorting instead.
     */
    public static NodeConnection createForDb(
        UUID uuid,
        Node node1,
        Node node2,
        NodeConnectionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    ) throws DatabaseException
    {

        return new NodeConnection(
            uuid,
            node1,
            node2,
            dbDriverRef,
            propsContainerFactory,
            transObjFactory,
            transMgrProviderRef
        );
    }

    public static @Nullable NodeConnection get(
        AccessContext accCtx,
        Node node1,
        Node node2
    )
        throws AccessDeniedException
    {
        Node source;
        Node target;
        if (node1.getName().compareTo(node2.getName()) < 0)
        {
            source = node1;
            target = node2;
        }
        else
        {
            source = node2;
            target = node1;
        }

        return source.getNodeConnection(accCtx, target);
    }

    public NodeName getSourceNodeName()
    {
        return sourceNode.getName();
    }

    public NodeName getTargetNodeName()
    {
        return targetNode.getName();
    }

    public Key getKey()
    {
        // no check deleted
        return nodeConnKey;
    }

    public Node getNode(AccessContext accCtx, NodeName nodeNameRef) throws AccessDeniedException
    {
        checkDeleted();

        Node node;
        if (sourceNode.getName().equals(nodeNameRef))
        {
            node = sourceNode;
        }
        else if (targetNode.getName().equals(nodeNameRef))
        {
            node = targetNode;
        }
        else
        {
            throw new ImplementationError(
                String.format("Given node name (%s) is neither source nor target node)", nodeNameRef.displayValue)
            );
        }
        node.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return node;
    }

    public Node getOtherNode(AccessContext accCtx, Node nodeRef) throws AccessDeniedException
    {
        checkDeleted();
        Node otherNode;
        if (sourceNode.equals(nodeRef))
        {
            otherNode = targetNode;
        }
        else if (targetNode.equals(nodeRef))
        {
            otherNode = sourceNode;
        }
        else
        {
            throw new ImplementationError("Node not part of nodeconnection: " + nodeRef);
        }
        otherNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        return otherNode;
    }

    public Node getSourceNode(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        sourceNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        targetNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return sourceNode;
    }

    public Node getTargetNode(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        sourceNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        targetNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return targetNode;
    }

    public NodeConnPojo getApiData(
        Node localNode,
        AccessContext accCtx,
        @Nullable Long fullSyncId,
        @Nullable Long updateId
    )
        throws AccessDeniedException
    {
        checkDeleted();
        sourceNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        targetNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);

        NodePojo otherNodePojo;
        if (sourceNode.equals(localNode))
        {
            otherNodePojo = targetNode.getApiData(accCtx, false, fullSyncId, updateId);
        }
        else if (targetNode.equals(localNode))
        {
            otherNodePojo = sourceNode.getApiData(accCtx, false, fullSyncId, updateId);
        }
        else
        {
            throw new ImplementationError(
                "Given localNode '" + localNode.getName() + "' is neither source node (" + sourceNode.getName() +
                    ") nor target node (" + targetNode.getName() + ")"
            );
        }
        return new NodeConnPojo(
            objId,
            localNode.getName().displayValue,
            otherNodePojo,
            props.map()
        );
    }

    public Props getProps(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(
            accCtx,
            sourceNode.getObjProt(),
            targetNode.getObjProt(),
            props
        );
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            sourceNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            targetNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            sourceNode.removeNodeConnection(accCtx, this);
            targetNode.removeNodeConnection(accCtx, this);

            props.delete();

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public int compareTo(NodeConnection other)
    {
        int cmp = sourceNode.compareTo(other.sourceNode);
        if (cmp == 0)
        {
            cmp = targetNode.compareTo(other.targetNode);
        }
        return cmp;
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(sourceNode, targetNode);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof NodeConnection)
        {
            NodeConnection other = (NodeConnection) obj;
            other.checkDeleted();
            ret = Objects.equals(sourceNode, other.sourceNode) && Objects.equals(targetNode, other.targetNode);
        }
        return ret;
    }

    @Override
    public String toStringImpl()
    {
        return "Node1: '" + nodeConnKey.sourceNodeName + "', " +
            "Node2: '" + nodeConnKey.targetNodeName + "'";
    }

    /**
     * Identifies a nodeConnection.
     */
    public static class Key implements Comparable<Key>
    {
        private final NodeName sourceNodeName;

        private final NodeName targetNodeName;

        public Key(NodeConnection nodeConn)
        {
            this(nodeConn.sourceNode.getName(), nodeConn.targetNode.getName());
        }

        public Key(NodeName sourceNodeNameRef, NodeName targetNodeNameRef)
        {
            sourceNodeName = sourceNodeNameRef;
            targetNodeName = targetNodeNameRef;
        }

        public NodeName getSourceNodeName()
        {
            return sourceNodeName;
        }

        public NodeName getTargetNodeName()
        {
            return targetNodeName;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sourceNodeName, targetNodeName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (!(obj instanceof Key))
            {
                return false;
            }
            Key other = (Key) obj;
            return Objects.equals(sourceNodeName, other.sourceNodeName) && Objects.equals(
                targetNodeName,
                other.targetNodeName
            );
        }

        @Override
        public int compareTo(Key other)
        {
            int eq = sourceNodeName.compareTo(other.sourceNodeName);
            if (eq == 0)
            {
                eq = targetNodeName.compareTo(other.targetNodeName);
            }
            return eq;
        }
    }
}
