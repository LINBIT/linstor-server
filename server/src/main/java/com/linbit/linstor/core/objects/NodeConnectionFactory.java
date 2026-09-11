package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.UUID;

public class NodeConnectionFactory
{
    private final NodeConnectionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NodeConnectionFactory(
        NodeConnectionDatabaseDriver dbDriverRef,
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

    public NodeConnection create(
        Node node1,
        Node node2
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {

        NodeConnection nodeConData = NodeConnection.createWithSorting(
            UUID.randomUUID(),
            node1,
            node2,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        dbDriver.create(nodeConData);

        node1.setNodeConnection(nodeConData);
        node2.setNodeConnection(nodeConData);

        return nodeConData;
    }

    public NodeConnection getInstanceSatellite(
        UUID uuid,
        Node node1,
        Node node2
    )
        throws ImplementationError
    {
        NodeConnection nodeConData = null;

        try
        {
            nodeConData = node1.getNodeConnection(node2);
            if (nodeConData == null)
            {
                nodeConData = NodeConnection.createWithSorting(
                    uuid,
                    node1,
                    node2,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );

                node1.setNodeConnection(nodeConData);
                node2.setNodeConnection(nodeConData);
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
