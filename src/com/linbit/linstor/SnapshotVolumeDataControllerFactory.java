package com.linbit.linstor;

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

    public SnapshotVolume create(
        AccessContext accCtx,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        SnapshotVolume snapshotVolume = driver.load(snapshot, snapshotVolumeDefinition, storPool, false);

        if (snapshotVolume != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolume already exists");
        }

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

        return snapshotVolume;
    }

    public SnapshotVolume load(
        AccessContext accCtx,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool
    )
        throws SQLException, AccessDeniedException
    {
        return driver.load(snapshot, snapshotVolumeDefinition, storPool, false);
    }
}
