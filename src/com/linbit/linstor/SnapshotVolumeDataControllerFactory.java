package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.UUID;

public class SnapshotVolumeDataControllerFactory
{
    private final SnapshotVolumeDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDataControllerFactory(
        SnapshotVolumeDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotVolume getInstance(
        AccessContext accCtx,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        SnapshotVolume snapshotVolume = snapshot.getSnapshotVolume(snapshotVolumeDefinition.getVolumeNumber());

        if (snapshotVolume != null && failIfExists)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolume already exists");
        }

        if (snapshotVolume == null && createIfNotExists)
        {
            snapshotVolume = new SnapshotVolumeData(
                UUID.randomUUID(),
                snapshot,
                snapshotVolumeDefinition,
                storPool,
                driver, transObjFactory, transMgrProvider
            );

            driver.create(snapshotVolume);
            snapshot.addSnapshotVolume(snapshotVolume);
            snapshotVolumeDefinition.addSnapshotVolume(snapshotVolume);
        }

        return snapshotVolume;
    }

    public SnapshotVolume load(
        AccessContext accCtx,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool
    )
        throws AccessDeniedException
    {
        SnapshotVolume instance;
        try
        {
            instance = getInstance(accCtx, snapshot, snapshotVolumeDefinition, storPool, false, false);
        }
        catch (LinStorDataAlreadyExistsException | SQLException exc)
        {
            throw new ImplementationError("Impossible exception was thrown", exc);
        }
        return instance;
    }
}
