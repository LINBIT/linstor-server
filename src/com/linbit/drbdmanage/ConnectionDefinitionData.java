package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

/**
 * Defines a connection for a DRBD resource
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ConnectionDefinitionData extends BaseTransactionObject implements ConnectionDefinition
{
    // Object identifier
    private final UUID objId;

    private final ObjectProtection objProt;

    private final ResourceDefinition resDfn;

    private final Node sourceNode;

    private final Node targetNode;

    private final ConnectionDefinitionDataDatabaseDriver dbDriver;

    private final int conNr;

    private boolean deleted = false;

    /*
     * used by getInstance
     */
    private ConnectionDefinitionData(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node1,
        Node node2,
        int conNrRef,
        TransactionMgr transMgr
    )
        throws SQLException, AccessDeniedException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                transMgr,
                ObjectProtection.buildPath(resDfn.getName(), node1.getName(), node2.getName()),
                true
            ),
            resDfn,
            node1,
            node2,
            conNrRef
        );
    }

    /*
     * used by dbDrivers and tests
     */
    ConnectionDefinitionData(
        UUID uuid,
        ObjectProtection objProtRef,
        ResourceDefinition resDfnRef,
        Node node1,
        Node node2,
        int conNrRef
    )
    {
        objId = uuid;
        objProt = objProtRef;
        resDfn = resDfnRef;

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
        conNr = conNrRef;


        dbDriver = DrbdManage.getConnectionDefinitionDatabaseDriver(
            resDfn.getName(),
            sourceNode.getName(),
            targetNode.getName()
        );

        transObjs = Arrays.asList(
            objProt,
            resDfn,
            sourceNode,
            targetNode
        );
    }

    public static ConnectionDefinitionData getInstance(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        Node node1,
        Node node2,
        int conNr,
        SerialGenerator srlGen,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws AccessDeniedException, SQLException
    {
        ConnectionDefinitionData conDfnData = null;

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

        ConnectionDefinitionDataDatabaseDriver dbDriver = DrbdManage.getConnectionDefinitionDatabaseDriver(
            resDfn.getName(),
            source.getName(),
            target.getName()
        );
        if (transMgr != null)
        {
            conDfnData = dbDriver.load(
                transMgr.dbCon,
                srlGen,
                transMgr
            );
        }

        if (conDfnData != null)
        {
            conDfnData.objProt.requireAccess(accCtx, AccessType.CONTROL);
            conDfnData.setConnection(transMgr);
        }
        else
        if (createIfNotExists)
        {
            conDfnData = new ConnectionDefinitionData(
                accCtx,
                resDfn,
                source,
                target,
                conNr,
                transMgr
            );
            if (transMgr != null)
            {
                dbDriver.create(transMgr.dbCon, conDfnData);
            }
        }

        if (conDfnData != null)
        {
            ((ResourceDefinitionData) resDfn).addConnection(accCtx, source.getName(), conNr, conDfnData);
            ((ResourceDefinitionData) resDfn).addConnection(accCtx, target.getName(), conNr, conDfnData);
            conDfnData.initialized();
        }
        return conDfnData;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public ResourceDefinition getResourceDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return resDfn;
    }

    @Override
    public Node getSourceNode(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return sourceNode;
    }

    @Override
    public Node getTargetNode(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return targetNode;
    }

    @Override
    public int getConnectionNumber(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return conNr;
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        dbDriver.delete(dbCon);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted connectionDefinition", null);
        }
    }
}
