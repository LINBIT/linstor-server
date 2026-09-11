package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDatabaseDriver;
import com.linbit.linstor.layer.snapshot.CtrlSnapLayerDataFactory;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import com.linbit.linstor.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.util.Map;
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
        Resource rsc,
        SnapshotDefinition snapshotDfn,
        Snapshot.Flags[] initFlags
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {

        Node node = rsc.getNode();
        Snapshot snapshot = snapshotDfn.getSnapshot(node.getName());

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
            new TreeMap<>(),
            null
        );

        driver.create(snapshot);
        snapshotDfn.addSnapshot(snapshot);
        node.addSnapshot(snapshot);

        snapLayerFactory.copyLayerData(rsc, snapshot);

        return snapshot;
    }

    public Snapshot restore(
        RscLayerDataApi layerData,
        Node node,
        SnapshotDefinition snapDfn,
        Snapshot.Flags[] initFlags,
        Map<String, String> renameStorPoolMap,
        @Nullable ApiCallRc apiCallRc
    ) throws LinStorDataAlreadyExistsException, DatabaseException
    {

        Snapshot snapshot = snapDfn.getSnapshot(node.getName());

        if (snapshot != null)
        {
            throw new LinStorDataAlreadyExistsException("The Snapshot already exists");
        }

        snapshot = new Snapshot(
            UUID.randomUUID(),
            snapDfn,
            node,
            StateFlagsBits.getMask(initFlags),
            driver,
            propsConFactory,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            null
        );

        driver.create(snapshot);
        snapDfn.addSnapshot(snapshot);
        node.addSnapshot(snapshot);

        snapLayerFactory.restoreLayerData(layerData, snapshot, renameStorPoolMap, apiCallRc);

        return snapshot;
    }
}
