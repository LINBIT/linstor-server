package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotDataControllerFactory
{
    private final SnapshotDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotDataControllerFactory(
        SnapshotDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public Snapshot create(
        AccessContext accCtx,
        Node node,
        SnapshotDefinition snapshotDfn,
        Snapshot.SnapshotFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        Snapshot snapshot = driver.load(node, snapshotDfn, false);

        if (snapshot != null)
        {
            throw new LinStorDataAlreadyExistsException("The Snapshot already exists");
        }

        snapshot = new SnapshotData(
            UUID.randomUUID(),
            snapshotDfn,
            node,
            StateFlagsBits.getMask(initFlags),
            driver, transObjFactory, transMgrProvider,
            new TreeMap<>()
        );

        driver.create(snapshot);
        snapshotDfn.addSnapshot(snapshot);
        node.addSnapshot(accCtx, snapshot);

        return snapshot;
    }

    public Snapshot load(
        AccessContext accCtx,
        Node node,
        SnapshotDefinition snapshotDfn
    )
        throws SQLException, AccessDeniedException
    {
        return driver.load(node, snapshotDfn, false);
    }
}
