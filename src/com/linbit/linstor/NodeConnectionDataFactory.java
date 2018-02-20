package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class NodeConnectionDataFactory
{
    private final NodeConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public NodeConnectionDataFactory(
        NodeConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public NodeConnectionData getInstance(
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
                UUID.randomUUID(),
                accCtx,
                source,
                target,
                dbDriver,
                transMgr,
                propsContainerFactory
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

    public NodeConnectionData getInstanceSatellite(
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
                    dbDriver, transMgr,
                    propsContainerFactory);
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
}
