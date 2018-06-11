package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDefinitionDataSatelliteFactory
{
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StorPoolDefinitionMap storPoolDfnMap;

    @Inject
    public StorPoolDefinitionDataSatelliteFactory(
        StorPoolDefinitionDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.StorPoolDefinitionMap storPooDfnMapRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        storPoolDfnMap = storPooDfnMapRef;
    }

    public StorPoolDefinitionData getInstance(
        AccessContext accCtx,
        UUID uuid,
        StorPoolName storPoolName
    )
        throws ImplementationError
    {
        StorPoolDefinitionData storPoolDfn = null;

        try
        {
            // we should be system-context here, so we skip the objProt-check
            storPoolDfn = (StorPoolDefinitionData) storPoolDfnMap.get(storPoolName);
            if (storPoolDfn == null)
            {
                storPoolDfn = new StorPoolDefinitionData(
                    uuid,
                    objectProtectionFactory.getInstance(
                        accCtx,
                        "",
                        true
                    ),
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
