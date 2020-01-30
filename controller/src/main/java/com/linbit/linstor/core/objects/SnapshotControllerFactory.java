package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.layer.snapshot.CtrlSnapLayerDataFactory;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class SnapshotControllerFactory
{
    private final SnapshotDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final PropsContainerFactory propsConFactory;
    private final CtrlSnapLayerDataFactory snapLayerFactory;

    @Inject
    public SnapshotControllerFactory(
        SnapshotDatabaseDriver driverRef,
        PropsContainerFactory propsConFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlSnapLayerDataFactory snapLayerFactoryRef
    )
    {
        driver = driverRef;
        propsConFactory = propsConFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        snapLayerFactory = snapLayerFactoryRef;
    }

    public Snapshot create(
        AccessContext accCtx,
        Resource rsc,
        SnapshotDefinition snapshotDfn,
        Snapshot.Flags[] initFlags
    )
        throws DatabaseException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        snapshotDfn.getResourceDefinition().getObjProt().requireAccess(accCtx, AccessType.USE);

        Node node = rsc.getNode();
        Snapshot snapshot = snapshotDfn.getSnapshot(accCtx, node.getName());

        if (snapshot != null)
        {
            throw new LinStorDataAlreadyExistsException("The Snapshot already exists");
        }

        snapshot = new Snapshot(
            UUID.randomUUID(),
            snapshotDfn,
            node,
            StateFlagsBits.getMask(initFlags),
            driver,
            propsConFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );

        driver.create(snapshot);
        snapshotDfn.addSnapshot(accCtx, snapshot);
        node.addSnapshot(accCtx, snapshot);

        snapLayerFactory.copyLayerData(rsc, snapshot);

        return snapshot;
    }
}
