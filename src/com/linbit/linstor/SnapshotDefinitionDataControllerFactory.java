package com.linbit.linstor;

import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
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

public class SnapshotDefinitionDataControllerFactory
{
    private final SnapshotDefinitionDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotDefinitionDataControllerFactory(
        SnapshotDefinitionDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotDefinitionData create(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.SnapshotDfnFlags[] initFlags
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotDefinitionData snapshotDfnData = driver.load(resDfn, snapshotName, false);

        if (snapshotDfnData != null)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotDefinition already exists");
        }

        snapshotDfnData = new SnapshotDefinitionData(
            UUID.randomUUID(),
            resDfn,
            snapshotName,
            StateFlagsBits.getMask(initFlags),
            driver,
            transObjFactory,
            transMgrProvider,
            new TreeMap<>(),
            new TreeMap<>()
        );

        driver.create(snapshotDfnData);
        resDfn.addSnapshotDfn(accCtx, snapshotDfnData);

        return snapshotDfnData;
    }

    public SnapshotDefinitionData load(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        SnapshotName snapshotName
    )
        throws SQLException, AccessDeniedException
    {
        resDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        return driver.load(resDfn, snapshotName, false);
    }
}
