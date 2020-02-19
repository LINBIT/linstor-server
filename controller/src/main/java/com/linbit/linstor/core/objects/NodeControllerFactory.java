package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.UUID;

@Singleton
public class NodeControllerFactory
{
    private final NodeDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final StorPoolControllerFactory storPoolFactory;
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
    public NodeControllerFactory(
        NodeDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        StorPoolControllerFactory storPoolFactoryRef,
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
        storPoolFactory = storPoolFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        freeSpaceMgrFactory = freeSpaceMgrFactoryRef;
        transMgrProvider = transMgrProviderRef;
        nodeRepository = nodeRepositoryRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
    }

    public Node create(
        AccessContext accCtx,
        NodeName nameRef,
        Node.Type type,
        Node.Flags[] flags
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        Node node = nodeRepository.get(accCtx, nameRef);

        if (node != null)
        {
            throw new LinStorDataAlreadyExistsException("The Node already exists");
        }

        node = new Node(
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
        dbDriver.create(node);
        node.setOfflinePeer(accCtx);

        return node;
    }
}
