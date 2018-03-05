package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.DisklessDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class NodeDataControllerFactory
{
    private final NodeDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolDataFactory storPoolDataFactory;
    private final StorPoolDefinition disklessStorPoolDfn;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public NodeDataControllerFactory(
        NodeDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        @Named(ControllerDbModule.DISKLESS_STOR_POOL_DFN) StorPoolDefinition disklessStorPoolDfnRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        disklessStorPoolDfn = disklessStorPoolDfnRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public NodeData getInstance(
        AccessContext accCtx,
        NodeName nameRef,
        Node.NodeType type,
        Node.NodeFlag[] flags,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        NodeData nodeData = null;

        nodeData = dbDriver.load(nameRef, false);

        if (failIfExists && nodeData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Node already exists");
        }

        if (nodeData == null && createIfNotExists)
        {
            nodeData = new NodeData(
                accCtx,
                UUID.randomUUID(),
                objectProtectionFactory.getInstance(
                    accCtx,
                    ObjectProtection.buildPath(nameRef),
                    true
                ),
                nameRef,
                type,
                StateFlagsBits.getMask(flags),
                dbDriver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            dbDriver.create(nodeData);

            nodeData.setDisklessStorPool(
                storPoolDataFactory.getInstance(
                    accCtx,
                    nodeData,
                    disklessStorPoolDfn,
                    DisklessDriver.class.getSimpleName(),
                    createIfNotExists,
                    failIfExists
                )
            );
        }
        if (nodeData != null)
        {
            nodeData.initialized();
        }
        return nodeData;
    }
}
