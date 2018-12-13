package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.interfaces.SnapshotDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
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
        NodeId nodeId,
        Snapshot.SnapshotFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        snapshotDfn.getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);

        Snapshot snapshot = snapshotDfn.getSnapshot(accCtx, node.getName());

        if (snapshot != null)
        {
            throw new LinStorDataAlreadyExistsException("The Snapshot already exists");
        }

        snapshot = new SnapshotData(
            UUID.randomUUID(),
            snapshotDfn,
            node,
            nodeId,
            StateFlagsBits.getMask(initFlags),
            driver, transObjFactory, transMgrProvider,
            new TreeMap<>()
            );

        driver.create(snapshot);
        snapshotDfn.addSnapshot(accCtx, snapshot);
        node.addSnapshot(accCtx, snapshot);

        return snapshot;
    }
}
