package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.UUID;

public class SnapshotVolumeControllerFactory
{
    private final SnapshotVolumeDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeControllerFactory(
        SnapshotVolumeDatabaseDriver driverRef,
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
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        snapshot.getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotVolume snapshotVolume = snapshot.getSnapshotVolume(accCtx, snapshotVolumeDefinition.getVolumeNumber());

        if (snapshotVolume != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolume already exists");
        }

        snapshotVolume = new SnapshotVolume(
            UUID.randomUUID(),
            snapshot,
            snapshotVolumeDefinition,
            storPool,
            driver, transObjFactory, transMgrProvider
        );

        driver.create(snapshotVolume);
        snapshot.addSnapshotVolume(accCtx, snapshotVolume);
        snapshotVolumeDefinition.addSnapshotVolume(accCtx, snapshotVolume);

        return snapshotVolume;
    }
}
