package com.linbit.linstor;

import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.ControllerDbModule;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.storage.DisklessDriver;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.SQLException;
import java.util.UUID;

public class NodeDataControllerFactory
{
    private final NodeDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolDataFactory storPoolDataFactory;
    private final StorPoolDefinition disklessStorPoolDfn;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public NodeDataControllerFactory(
        NodeDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolDataFactory storPoolDataFactoryRef,
        @Named(ControllerDbModule.DISKLESS_STOR_POOL_DFN) StorPoolDefinition disklessStorPoolDfnRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        disklessStorPoolDfn = disklessStorPoolDfnRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public NodeData getInstance(
        AccessContext accCtx,
        NodeName nameRef,
        Node.NodeType type,
        Node.NodeFlag[] flags,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        NodeData nodeData = null;

        nodeData = dbDriver.load(nameRef, false, transMgr);

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
                    true,
                    transMgr
                ),
                nameRef,
                type,
                StateFlagsBits.getMask(flags),
                transMgr,
                dbDriver,
                propsContainerFactory
            );
            dbDriver.create(nodeData, transMgr);

            nodeData.setDisklessStorPool(storPoolDataFactory.getInstance(
                accCtx,
                nodeData,
                disklessStorPoolDfn,
                DisklessDriver.class.getSimpleName(),
                transMgr,
                createIfNotExists,
                failIfExists
            ));
        }
        if (nodeData != null)
        {
            nodeData.initialized();
            nodeData.setConnection(transMgr);
        }
        return nodeData;
    }
}
