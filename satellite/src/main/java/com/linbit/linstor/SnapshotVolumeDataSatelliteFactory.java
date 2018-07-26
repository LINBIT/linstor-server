package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.UUID;

public class SnapshotVolumeDataSatelliteFactory
{
    private final SnapshotVolumeDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDataSatelliteFactory(
        SnapshotVolumeDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotVolume getInstanceSatellite(
        AccessContext accCtx,
        UUID snapshotVolumeUuid,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        StorPool storPool
    )
        throws ImplementationError
    {
        SnapshotVolume snapshotVolume;
        try
        {
            snapshotVolume = snapshot.getSnapshotVolume(accCtx, snapshotVolumeDefinition.getVolumeNumber());
            if (snapshotVolume == null)
            {
                snapshotVolume = new SnapshotVolumeData(
                    snapshotVolumeUuid,
                    snapshot,
                    snapshotVolumeDefinition,
                    storPool,
                    driver, transObjFactory, transMgrProvider
                );
                snapshot.addSnapshotVolume(accCtx, snapshotVolume);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return snapshotVolume;
    }
}
