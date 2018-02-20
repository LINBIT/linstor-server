package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;

import javax.inject.Inject;
import java.sql.SQLException;
import java.util.UUID;

public class StorPoolDefinitionDataFactory
{
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final ObjectProtectionFactory objectProtectionFactory;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public StorPoolDefinitionDataFactory(
        StorPoolDefinitionDataDatabaseDriver dbDriverRef,
        ObjectProtectionFactory objectProtectionFactoryRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        dbDriver = dbDriverRef;
        objectProtectionFactory = objectProtectionFactoryRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    public StorPoolDefinitionData getInstance(
        AccessContext accCtx,
        StorPoolName nameRef,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, LinStorDataAlreadyExistsException
    {
        StorPoolDefinitionData storPoolDfn = null;

        storPoolDfn = dbDriver.load(nameRef, false, transMgr);

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
                    ObjectProtection.buildPathSPD(nameRef),
                    true,
                    transMgr
                ),
                nameRef,
                dbDriver,
                propsContainerFactory
            );

            dbDriver.create(storPoolDfn, transMgr);
        }

        if (storPoolDfn != null)
        {
            storPoolDfn.initialized();
            storPoolDfn.setConnection(transMgr);
        }

        return storPoolDfn;
    }

    public StorPoolDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        StorPoolName nameRef,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        StorPoolDefinitionData storPoolDfn = null;

        try
        {
            storPoolDfn = dbDriver.load(nameRef, false, transMgr);
            if (storPoolDfn == null)
            {
                storPoolDfn = new StorPoolDefinitionData(
                    uuid,
                    objectProtectionFactory.getInstance(
                        accCtx,
                        "",
                        true,
                        transMgr
                    ),
                    nameRef,
                    dbDriver,
                    propsContainerFactory
                );
            }
            storPoolDfn.initialized();
            storPoolDfn.setConnection(transMgr);
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
