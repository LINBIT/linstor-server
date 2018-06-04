package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.sql.SQLException;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotVolumeDefinitionControllerFactory
{
    private final SnapshotVolumeDefinitionDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotVolumeDefinitionControllerFactory(
        SnapshotVolumeDefinitionDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotVolumeDefinition create(
        AccessContext accCtx,
        SnapshotDefinition snapshotDfn,
        VolumeNumber volumeNumber
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        SnapshotVolumeDefinition snapshotVolumeDefinition =
            driver.load(snapshotDfn, volumeNumber, false);

        if (snapshotVolumeDefinition != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolumeDefinition already exists");
        }

        snapshotVolumeDefinition = new SnapshotVolumeDefinitionData(
            UUID.randomUUID(),
            snapshotDfn,
            volumeNumber,
            driver,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>()
        );

        driver.create(snapshotVolumeDefinition);
        snapshotDfn.addSnapshotVolumeDefinition(snapshotVolumeDefinition);

        return snapshotVolumeDefinition;
    }

    public SnapshotVolumeDefinition load(
        AccessContext accCtx,
        SnapshotDefinition snapshotDfn,
        VolumeNumber volumeNumber
    )
        throws SQLException, AccessDeniedException
    {
        return driver.load(snapshotDfn, volumeNumber, false);
    }
}
