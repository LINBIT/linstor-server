package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class StorPoolDataFactory
{
    private final StorPoolDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolDataFactory(
        StorPoolDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public StorPoolData getInstance(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        nodeRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDefRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPoolData storPoolData = null;

        storPoolData = driver.load(nodeRef, storPoolDefRef, false);

        if (failIfExists && storPoolData != null)
        {
            throw new LinStorDataAlreadyExistsException("The StorPool already exists");
        }

        if (storPoolData == null && createIfNotExists)
        {
            storPoolData = new StorPoolData(
                UUID.randomUUID(),
                accCtx,
                nodeRef,
                storPoolDefRef,
                storDriverSimpleClassNameRef,
                false,
                driver,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            driver.create(storPoolData);
        }
        if (storPoolData != null)
        {
            storPoolData.initialized();
        }
        return storPoolData;
    }

    public StorPoolData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef
    )
        throws ImplementationError
    {
        StorPoolData storPoolData = null;

        try
        {
            storPoolData = driver.load(nodeRef, storPoolDefRef, false);
            if (storPoolData == null)
            {
                storPoolData = new StorPoolData(
                    uuid,
                    accCtx,
                    nodeRef,
                    storPoolDefRef,
                    storDriverSimpleClassNameRef,
                    true,
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
            }
            storPoolData.initialized();
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }


        return storPoolData;
    }
}
