package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.PropsAccess;
import com.linbit.drbdmanage.propscon.PropsContainer;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;

/**
 * Defines a connection between two DRBD nodes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NodeConnectionData extends BaseTransactionObject implements NodeConnection
{
    // Object identifier
    private final UUID objId;
    private final Node sourceNode;
    private final Node targetNode;

    private final Props props;

    private final NodeConnectionDataDatabaseDriver dbDriver;

    private boolean deleted = false;

    /*
     * used by getInstance
     */
    private NodeConnectionData(
        Node node1,
        Node node2,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        this(
            UUID.randomUUID(),
            node1,
            node2,
            transMgr
        );
    }

    /*
     * used by dbDrivers and tests
     */
    NodeConnectionData(
        UUID uuid,
        Node node1,
        Node node2,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        objId = uuid;

        if (node1.getName().compareTo(node2.getName()) < 0)
        {
            sourceNode = node1;
            targetNode = node2;
        }
        else
        {
            sourceNode = node2;
            targetNode = node1;
        }

        props = PropsContainer.getInstance(
            PropsContainer.buildPath(
                sourceNode.getName(),
                targetNode.getName()
            ),
            transMgr
        );

        dbDriver = DrbdManage.getNodeConnectionDatabaseDriver();

        transObjs = Arrays.asList(
            sourceNode,
            targetNode,
            props
        );
    }

    public static NodeConnectionData getInstance(
        AccessContext accCtx,
        Node node1,
        Node node2,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws AccessDeniedException, SQLException
    {
        NodeConnectionData nodeConDfnData = null;

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

        NodeConnectionDataDatabaseDriver dbDriver = DrbdManage.getNodeConnectionDatabaseDriver();

        if (transMgr != null)
        {
            nodeConDfnData = dbDriver.load(
                source,
                target,
                transMgr
            );
        }

        if (nodeConDfnData != null)
        {
            source.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            target.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            nodeConDfnData.setConnection(transMgr);
        }
        else
        if (createIfNotExists)
        {
            nodeConDfnData = new NodeConnectionData(
                source,
                target,
                transMgr
            );
            if (transMgr != null)
            {
                dbDriver.create(nodeConDfnData, transMgr);
            }
        }

        if (nodeConDfnData != null)
        {
            source.setNodeConnection(accCtx, nodeConDfnData);
            target.setNodeConnection(accCtx, nodeConDfnData);

            nodeConDfnData.initialized();
        }
        return nodeConDfnData;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public Node getSourceNode(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        sourceNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return sourceNode;
    }

    @Override
    public Node getTargetNode(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        sourceNode.getObjProt().requireAccess(accCtx, AccessType.VIEW);
        return targetNode;
    }

    @Override
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
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        sourceNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        targetNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        sourceNode.removeNodeConnection(accCtx, this);
        targetNode.removeNodeConnection(accCtx, this);

        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted NodeConnection", null);
        }
    }
}
