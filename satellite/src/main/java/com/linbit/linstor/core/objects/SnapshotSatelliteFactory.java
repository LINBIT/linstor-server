package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.TreeMap;
import java.util.UUID;

public class SnapshotSatelliteFactory
{
    private final SnapshotDatabaseDriver driver;
    private final PropsContainerFactory propsConFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotSatelliteFactory(
        SnapshotDatabaseDriver driverRef,
        PropsContainerFactory propsConFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        propsConFactory = propsConFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public Snapshot getInstanceSatellite(
        AccessContext accCtx,
        UUID snapshotUuid,
        Node node,
        SnapshotDefinition snapshotDfn,
        Snapshot.Flags[] flags
    )
        throws ImplementationError
    {
        Snapshot snapshot;
        try
        {
            snapshot = snapshotDfn.getSnapshot(accCtx, node.getName());
            if (snapshot == null)
            {
                snapshot = new Snapshot(
                    snapshotUuid,
                    snapshotDfn,
                    node,
                    StateFlagsBits.getMask(flags),
                    driver,
                    propsConFactory,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    null
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
