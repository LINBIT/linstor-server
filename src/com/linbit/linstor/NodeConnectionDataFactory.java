package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class NodeConnectionDataFactory
{
    private final NodeConnectionDataDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NodeConnectionDataFactory(
        NodeConnectionDataDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public NodeConnectionData getInstance(
        AccessContext accCtx,
        Node node1,
        Node node2,
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

        nodeConData = dbDriver.load(source, target, false);

        if (failIfExists && nodeConData != null)
        {
            throw new LinStorDataAlreadyExistsException("The NodeConnection already exists");
        }

        if (nodeConData == null && createIfNotExists)
        {
            nodeConData = new NodeConnectionData(
                UUID.randomUUID(),
                source,
                target,
                dbDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            dbDriver.create(nodeConData);

            source.setNodeConnection(accCtx, nodeConData);
            target.setNodeConnection(accCtx, nodeConData);
        }
        return nodeConData;
    }

    public NodeConnectionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node node1,
        Node node2
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
            nodeConData = dbDriver.load(source, target, false);
            if (nodeConData == null)
            {
                nodeConData = new NodeConnectionData(
                    uuid,
                    source,
                    target,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );

                source.setNodeConnection(accCtx, nodeConData);
                target.setNodeConnection(accCtx, nodeConData);
            }
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
