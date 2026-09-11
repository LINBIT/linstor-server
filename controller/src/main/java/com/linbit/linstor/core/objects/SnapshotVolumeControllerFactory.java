package com.linbit.linstor.core.objects;

import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDatabaseDriver;
import com.linbit.linstor.layer.snapshot.CtrlSnapLayerDataFactory;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import com.linbit.linstor.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Map;
import java.util.UUID;

public class SnapshotVolumeControllerFactory
{
    private final SnapshotVolumeDatabaseDriver driver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;
    private final CtrlSnapLayerDataFactory snapLayerFactory;

    @Inject
    public SnapshotVolumeControllerFactory(
        SnapshotVolumeDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef,
        CtrlSnapLayerDataFactory snapLayerFactoryRef
    )
    {
        driver = driverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
        snapLayerFactory = snapLayerFactoryRef;
    }

    public SnapshotVolume create(
        Resource rsc,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {

        SnapshotVolume snapshotVolume = snapshot.getVolume(snapshotVolumeDefinition.getVolumeNumber());

        if (snapshotVolume != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolume already exists");
        }

        snapshotVolume = new SnapshotVolume(
            UUID.randomUUID(),
            snapshot,
            snapshotVolumeDefinition,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        driver.create(snapshotVolume);
        snapshot.putVolume(snapshotVolume);
        snapshotVolumeDefinition.addSnapshotVolume(snapshotVolume);

        snapLayerFactory.copyLayerData(rsc, snapshot); // create layerdata for new SnapshotVolume

        return snapshotVolume;
    }

    public SnapshotVolume restore(
        RscLayerDataApi layerData,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition,
        Map<String, String> renameStorPoolsMap,
        @Nullable ApiCallRc apiCallRc
    )
        throws DatabaseException, LinStorDataAlreadyExistsException
    {

        SnapshotVolume snapshotVolume = snapshot.getVolume(snapshotVolumeDefinition.getVolumeNumber());

        if (snapshotVolume != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolume already exists");
        }

        snapshotVolume = new SnapshotVolume(
            UUID.randomUUID(),
            snapshot,
            snapshotVolumeDefinition,
            driver,
            propsContainerFactory,
            transObjFactory,
            transMgrProvider
        );

        driver.create(snapshotVolume);
        snapshot.putVolume(snapshotVolume);
        snapshotVolumeDefinition.addSnapshotVolume(snapshotVolume);

        snapLayerFactory.restoreLayerData(layerData, snapshot, renameStorPoolsMap, apiCallRc);
        return snapshotVolume;
    }
}
