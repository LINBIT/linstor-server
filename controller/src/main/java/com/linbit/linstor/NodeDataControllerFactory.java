package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
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
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.UUID;

@Singleton
public class NodeDataControllerFactory
{
    private final NodeDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolDataControllerFactory storPoolDataFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final FreeSpaceMgrControllerFactory freeSpaceMgrFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final NodeRepository nodeRepository;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;

    private static final StorPoolName DISKLESS_STOR_POOL_NAME;

    static
    {
        try
        {
            DISKLESS_STOR_POOL_NAME = new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME);
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError("Default diskless stor pool name not valid");
        }
    }

    @Inject
    public NodeDataControllerFactory(
        NodeDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolDataControllerFactory storPoolDataFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        FreeSpaceMgrControllerFactory freeSpaceMgrFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        NodeRepository nodeRepositoryRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef,
        @SystemContext AccessContext sysCtx
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        storPoolDataFactory = storPoolDataFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nodeRepository = nodeRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
    }

    public NodeData create(
        AccessContext accCtx,
        NodeName nameRef,
        Node.NodeType type,
        Node.NodeFlag[] flags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        NodeData nodeData = nodeRepository.get(accCtx, nameRef);

        if (nodeData != null)
        {
            throw new LinStorDataAlreadyExistsException("The Node already exists");
        }

        nodeData = new NodeData(
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
            storPoolDataFactory.create(
                accCtx,
                nodeData,
                storPoolDefinitionRepository.get(accCtx, DISKLESS_STOR_POOL_NAME),
                DisklessDriver.class.getSimpleName(),
                freeSpaceMgrFactory.getInstance(accCtx, new FreeSpaceMgrName(nameRef, DISKLESS_STOR_POOL_NAME))
            )
        );

        return nodeData;
    }
}
