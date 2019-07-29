package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDefinitionDataControllerFactory
{
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StorPoolDefinitionRepository storPoolDefinitionRepository;

    @Inject
    public StorPoolDefinitionDataControllerFactory(
        StorPoolDefinitionDataDatabaseDriver dbDriverRef,
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

    public StorPoolDefinitionData create(
        AccessContext accCtx,
        StorPoolName storPoolName
    )
        throws AccessDeniedException, DatabaseException, LinStorDataAlreadyExistsException
    {
        StorPoolDefinitionData storPoolDfn = null;

        storPoolDfn = storPoolDefinitionRepository.get(accCtx, storPoolName);

        if (storPoolDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPoolDefinition already exists");
        }

        storPoolDfn = new StorPoolDefinitionData(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPathSPD(storPoolName),
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
