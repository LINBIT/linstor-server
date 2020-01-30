package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.UUID;

public class SnapshotVolumeSatelliteFactory
{
    private final PropsContainerFactory propsContainerFactory;
    private final SnapshotVolumeDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeSatelliteFactory(
        PropsContainerFactory propsContainerFactoryRef,
        SnapshotVolumeDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        propsContainerFactory = propsContainerFactoryRef;
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotVolume getInstanceSatellite(
        AccessContext accCtx,
        UUID snapshotVolumeUuid,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
        throws ImplementationError
    {
        SnapshotVolume snapshotVolume;
        try
        {
            snapshotVolume = snapshot.getVolume(snapshotVolumeDefinition.getVolumeNumber());
            if (snapshotVolume == null)
            {
                snapshotVolume = new SnapshotVolume(
                    snapshotVolumeUuid,
                    snapshot,
                    snapshotVolumeDefinition,
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider
                );
                snapshot.putVolume(accCtx, snapshotVolume);
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
