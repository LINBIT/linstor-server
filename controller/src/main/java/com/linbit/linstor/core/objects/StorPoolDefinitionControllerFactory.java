package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDefinitionControllerFactory
{
    private final StorPoolDefinitionDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;

    @Inject
    public StorPoolDefinitionControllerFactory(
        StorPoolDefinitionDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
    }

    public StorPoolDefinition create(
        AccessContext accCtx,
        StorPoolName storPoolName
    )
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        StorPoolDefinition storPoolDfn = null;

        storPoolDfn = storPoolDefinitionRepository.get(accCtx, storPoolName);

        if (storPoolDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPoolDefinition already exists");
        }

        storPoolDfn = new StorPoolDefinition(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(storPoolName),
                true
            ),
            storPoolName,
            dbDriver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );

        dbDriver.create(storPoolDfn);

        return storPoolDfn;
    }
}
