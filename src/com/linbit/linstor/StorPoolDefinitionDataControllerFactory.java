package com.linbit.linstor;

import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ControllerSecurityModule;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class StorPoolDefinitionDataControllerFactory
{
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final ObjectProtection storPoolDfnMapProt;

    @Inject
    public StorPoolDefinitionDataControllerFactory(
        StorPoolDefinitionDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CoreModule.StorPoolDefinitionMap storPooDfnMapRef,
        @Named(ControllerSecurityModule.STOR_POOL_DFN_MAP_PROT) ObjectProtection storPoolDfnMapProtRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        storPoolDfnMap = storPooDfnMapRef;
        storPoolDfnMapProt = storPoolDfnMapProtRef;
    }

    public StorPoolDefinitionData getInstance(
        AccessContext accCtx,
        StorPoolName storPoolName,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, LinStorDataAlreadyExistsException
    {
        StorPoolDefinitionData storPoolDfn = null;

        storPoolDfnMapProt.requireAccess(accCtx, AccessType.VIEW);
        storPoolDfn = (StorPoolDefinitionData) storPoolDfnMap.get(storPoolName);

        if (failIfExists && storPoolDfn != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPoolDefinition already exists");
        }

        if (storPoolDfn == null && createIfNotExists)
        {
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
        }

        return storPoolDfn;
    }
}
