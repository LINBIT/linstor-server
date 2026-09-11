package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDefinitionControllerFactory
{
    private final StorPoolDefinitionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;

    @Inject
    public StorPoolDefinitionControllerFactory(
        StorPoolDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        StorPoolDefinitionRepository storPoolDefinitionRepositoryRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        storPoolDefinitionRepository = storPoolDefinitionRepositoryRef;
    }

    public StorPoolDefinition create(
        StorPoolName storPoolName
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        StorPoolDefinition storPoolDfn = storPoolDefinitionRepository.get(storPoolName);

        if (storPoolDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPoolDefinition already exists");
        }

        storPoolDfn = new StorPoolDefinition(
            UUID.randomUUID(),
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
