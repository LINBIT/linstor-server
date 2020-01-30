package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class SnapshotVolumeDefinitionSatelliteFactory
{
    private final SnapshotVolumeDefinitionDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDefinitionSatelliteFactory(
        SnapshotVolumeDefinitionDatabaseDriver driverRef,
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

    public SnapshotVolumeDefinition getInstanceSatellite(
        AccessContext accCtx,
        UUID snapshotVolumeDefinitionUuid,
        SnapshotDefinition snapshotDfn,
        VolumeDefinition vlmDfn,
        VolumeNumber vlmNr,
        long vlmSize,
        SnapshotVolumeDefinition.Flags[] flags
    )
        throws ImplementationError
    {
        SnapshotVolumeDefinition snapshotVolumeDefinition;
        try
        {
            snapshotVolumeDefinition = snapshotDfn.getSnapshotVolumeDefinition(accCtx, vlmNr);
            if (snapshotVolumeDefinition == null)
            {
                snapshotVolumeDefinition = new SnapshotVolumeDefinition(
                    snapshotVolumeDefinitionUuid,
                    snapshotDfn,
                    vlmDfn,
                    vlmNr,
                    vlmSize,
                    StateFlagsBits.getMask(flags),
                    driver,
                    propsContainerFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>()
                );
                snapshotDfn.addSnapshotVolumeDefinition(accCtx, snapshotVolumeDefinition);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return snapshotVolumeDefinition;
    }
}
