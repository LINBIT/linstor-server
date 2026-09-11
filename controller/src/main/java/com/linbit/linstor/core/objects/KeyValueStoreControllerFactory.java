package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import java.util.UUID;

@Singleton
public class KeyValueStoreControllerFactory
{
    private final KeyValueStoreDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final KeyValueStoreRepository kvsRepository;

    @Inject
    public KeyValueStoreControllerFactory(
        KeyValueStoreDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        KeyValueStoreRepository keyValueStoreRepositoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        kvsRepository = keyValueStoreRepositoryRef;
    }

    public KeyValueStore create(
        KeyValueStoreName kvsName
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {
        KeyValueStore kvs = kvsRepository.get(kvsName);

        if (kvs != null)
        {
            throw new LinStorDataAlreadyExistsException("The KeyValueStore already exists");
        }

        kvs = new KeyValueStore(
            UUID.randomUUID(),
            kvsName,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );
        driver.create(kvs);

        return kvs;
    }
}
