package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.StorageDriverKind;
import com.linbit.linstor.storage.StorageDriverLoader;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class StorPoolDataFactory
{
    private final StorPoolDataDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public StorPoolDataFactory(
        StorPoolDataDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public StorPoolData getInstance(
        AccessContext accCtx,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        nodeRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        storPoolDefRef.getObjProt().requireAccess(accCtx, AccessType.USE);
        StorPoolData storPoolData = null;

        storPoolData = driver.load(nodeRef, storPoolDefRef, false, transMgr);

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
                transMgr,
                driver,
                propsContainerFactory
            );
            driver.create(storPoolData, transMgr);
        }
        if (storPoolData != null)
        {
            storPoolData.initialized();
            storPoolData.setConnection(transMgr);
        }
        return storPoolData;
    }

    public StorPoolData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        Node nodeRef,
        StorPoolDefinition storPoolDefRef,
        String storDriverSimpleClassNameRef,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        StorPoolData storPoolData = null;

        try
        {
            storPoolData = driver.load(nodeRef, storPoolDefRef, false, transMgr);
            if (storPoolData == null)
            {
                storPoolData = new StorPoolData(
                    uuid,
                    accCtx,
                    nodeRef,
                    storPoolDefRef,
                    storDriverSimpleClassNameRef,
                    true,
                    transMgr,
                    driver,
                    propsContainerFactory
                );
            }
            storPoolData.initialized();
            storPoolData.setConnection(transMgr);
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
