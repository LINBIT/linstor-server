package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotDataSatelliteFactory
{
    private final SnapshotDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotDataSatelliteFactory(
        SnapshotDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public Snapshot getInstanceSatellite(
        AccessContext accCtx,
        UUID snapshotUuid,
        Node node,
        SnapshotDefinition snapshotDfn,
        Snapshot.SnapshotFlags[] flags
    )
        throws ImplementationError
    {
        Snapshot snapshot;
        try
        {
            snapshot = snapshotDfn.getSnapshot(accCtx, node.getName());
            if (snapshot == null)
            {
                snapshot = new SnapshotData(
                    snapshotUuid,
                    snapshotDfn,
                    node,
                    // Snapshot node ID is not relevant for the satellite
                    null,
                    StateFlagsBits.getMask(flags),
                    driver, transObjFactory, transMgrProvider,
                    new TreeMap<>()
                );
                snapshotDfn.addSnapshot(accCtx, snapshot);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return snapshot;
    }
}
