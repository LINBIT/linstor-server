package com.linbit.linstor;

import com.linbit.drbd.md.MdException;
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

    public SnapshotVolumeDefinition getInstance(
        AccessContext accCtx,
        SnapshotDefinition snapshotDfn,
        VolumeNumber volumeNumber,
        long volSize,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException, MdException
    {
        SnapshotVolumeDefinition snapshotVolumeDefinition = load(snapshotDfn, volumeNumber);

        if (snapshotVolumeDefinition != null && failIfExists)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotVolumeDefinition already exists");
        }

        if (snapshotVolumeDefinition == null && createIfNotExists)
        {
            snapshotVolumeDefinition = new SnapshotVolumeDefinitionData(
                UUID.randomUUID(),
                snapshotDfn,
                volumeNumber,
                volSize,
                driver,
                transObjFactory,
                transMgrProvider,
                new TreeMap<>()
            );

            driver.create(snapshotVolumeDefinition);
            snapshotDfn.addSnapshotVolumeDefinition(snapshotVolumeDefinition);
        }

        return snapshotVolumeDefinition;
    }

    public SnapshotVolumeDefinition load(
        SnapshotDefinition snapshotDfn,
        VolumeNumber volumeNumber
    )
    {
        return snapshotDfn.getSnapshotVolumeDefinition(volumeNumber);
    }
}
