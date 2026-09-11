package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDefinitionSatelliteFactory
{
    private final StorPoolDefinitionDatabaseDriver dbDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StorPoolDefinitionMap storPoolDfnMap;

    @Inject
    public StorPoolDefinitionSatelliteFactory(
        StorPoolDefinitionDatabaseDriver dbDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.StorPoolDefinitionMap storPooDfnMapRef
    )
    {
        dbDriver = dbDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        storPoolDfnMap = storPooDfnMapRef;
    }

    public StorPoolDefinition getInstance(
        UUID uuid,
        StorPoolName storPoolName
    )
        throws ImplementationError
    {
        StorPoolDefinition storPoolDfn = null;

        try
        {
            storPoolDfn = (StorPoolDefinition) storPoolDfnMap.get(storPoolName);
            if (storPoolDfn == null)
            {
                storPoolDfn = new StorPoolDefinition(
                    uuid,
                    storPoolName,
                    dbDriver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>()
                );
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return storPoolDfn;
    }
}
