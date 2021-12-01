package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.repository.KeyValueStoreRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
public class KeyValueStoreControllerFactory
{
    private final KeyValueStoreDatabaseDriver driver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final KeyValueStoreRepository kvsRepository;

    @Inject
    public KeyValueStoreControllerFactory(
        KeyValueStoreDatabaseDriver driverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        KeyValueStoreRepository keyValueStoreRepositoryRef
    )
    {
        driver = driverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        kvsRepository = keyValueStoreRepositoryRef;
    }

    public KeyValueStore create(
        AccessContext accCtx,
        KeyValueStoreName kvsName
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        KeyValueStore kvs = kvsRepository.get(accCtx, kvsName);

        if (kvs != null)
        {
            throw new LinStorDataAlreadyExistsException("The KeyValueStore already exists");
        }

        kvs = new KeyValueStore(
            UUID.randomUUID(),
            objectProtectionFactory.getInstance(
                accCtx,
                ObjectProtection.buildPath(kvsName),
                true
            ),
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
