package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.TransactionSimpleObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

/**
 * Defines a connection between two LinStor nodes
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class NodeConnectionData extends BaseTransactionObject implements NodeConnection
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Node sourceNode;
    private final Node targetNode;

    private final Props props;

    private final NodeConnectionDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<NodeConnectionData, Boolean> deleted;

    /*
     * used by getInstance
     */
    private NodeConnectionData(
        AccessContext accCtx,
        Node node1,
        Node node2,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            accCtx,
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
        AccessContext accCtx,
        Node node1,
        Node node2,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        objId = uuid;
        dbgInstanceId = UUID.randomUUID();

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
        deleted = new TransactionSimpleObject<>(this, false, null);

        dbDriver = LinStor.getNodeConnectionDatabaseDriver();

        transObjs = Arrays.asList(
            sourceNode,
            targetNode,
            props,
            deleted
        );
        sourceNode.setNodeConnection(accCtx, this);
        targetNode.setNodeConnection(accCtx, this);
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public static NodeConnectionData getInstance(
        AccessContext accCtx,
        Node node1,
        Node node2,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, LinStorDataAlreadyExistsException
    {
        NodeConnectionData nodeConData = null;

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

        source.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        target.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

        NodeConnectionDataDatabaseDriver dbDriver = LinStor.getNodeConnectionDatabaseDriver();
        nodeConData = dbDriver.load(
            source,
            target,
            false,
            transMgr
        );

        if (failIfExists && nodeConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The NodeConnection already exists");
        }

        if (nodeConData == null && createIfNotExists)
        {
            nodeConData = new NodeConnectionData(
                accCtx,
                source,
                target,
                transMgr
            );
            dbDriver.create(nodeConData, transMgr);
        }
        if (nodeConData != null)
        {
            nodeConData.initialized();
            nodeConData.setConnection(transMgr);
        }
        return nodeConData;
    }

    public static NodeConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node1,
        Node node2,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        NodeConnectionData nodeConData = null;

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

        NodeConnectionDataDatabaseDriver dbDriver = LinStor.getNodeConnectionDatabaseDriver();
        try
        {
            nodeConData = dbDriver.load(
                source,
                target,
                false,
                transMgr
            );
            if (nodeConData == null)
            {
                nodeConData = new NodeConnectionData(
                    uuid,
                    accCtx,
                    source,
                    target,
                    transMgr
                );
            }
            nodeConData.initialized();
            nodeConData.setConnection(transMgr);
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return nodeConData;
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
        if (!deleted.get())
        {
            sourceNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);
            targetNode.getObjProt().requireAccess(accCtx, AccessType.CHANGE);

            sourceNode.removeNodeConnection(accCtx, this);
            targetNode.removeNodeConnection(accCtx, this);

            dbDriver.delete(this, transMgr);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new ImplementationError("Access to deleted node connection", null);
        }
    }

    @Override
    public String toString()
    {
        return "Node1: '" + sourceNode.getName() + "', " +
               "Node2: '" + targetNode.getName() + "'";
    }
}
