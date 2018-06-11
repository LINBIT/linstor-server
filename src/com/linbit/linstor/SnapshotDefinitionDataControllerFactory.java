package com.linbit.linstor;

import com.linbit.ImplementationError;
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

    public SnapshotDefinitionData getInstance(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.SnapshotDfnFlags[] initFlags,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws SQLException, AccessDeniedException, LinStorDataAlreadyExistsException
    {
        rscDfn.getObjProt().requireAccess(accCtx, AccessType.USE);

        SnapshotDefinitionData snapshotDfnData = (SnapshotDefinitionData) rscDfn.getSnapshotDfn(accCtx, snapshotName);

        if (snapshotDfnData != null && failIfExists)
        {
            throw new LinStorDataAlreadyExistsException("The SnapshotDefinition already exists");
        }

        if (snapshotDfnData == null && createIfNotExists)
        {
            snapshotDfnData = new SnapshotDefinitionData(
                UUID.randomUUID(),
                rscDfn,
                snapshotName,
                StateFlagsBits.getMask(initFlags),
                driver,
                transObjFactory,
                transMgrProvider,
                new TreeMap<>(),
                new TreeMap<>()
            );

            driver.create(snapshotDfnData);
            rscDfn.addSnapshotDfn(accCtx, snapshotDfnData);
        }

        return snapshotDfnData;
    }

    public SnapshotDefinitionData load(
        AccessContext accCtx,
        ResourceDefinition resDfn,
        SnapshotName snapshotName
    )
        throws AccessDeniedException
    {
        SnapshotDefinitionData snapshotDfn;
        try
        {
            snapshotDfn = getInstance(accCtx, resDfn, snapshotName, null, false, false);
        }
        catch (
            LinStorDataAlreadyExistsException |
            SQLException exc
        )
        {
            throw new ImplementationError("Impossible Exception was thrown", exc);
        }
        return snapshotDfn;
    }
}
