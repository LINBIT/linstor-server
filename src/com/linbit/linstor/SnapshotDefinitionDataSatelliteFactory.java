package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotDefinitionDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.stateflags.StateFlagsBits;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.TreeMap;
import java.util.UUID;

public class SnapshotDefinitionDataSatelliteFactory
{
    private final SnapshotDefinitionDataDatabaseDriver driver;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public SnapshotDefinitionDataSatelliteFactory(
        SnapshotDefinitionDataDatabaseDriver driverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        driver = driverRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    public SnapshotDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID snapshotDfnUuid,
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.SnapshotDfnFlags[] flags
    )
        throws ImplementationError
    {
        SnapshotDefinitionData snapshotDfnData;
        try
        {
            snapshotDfnData = driver.load(rscDfn, snapshotName, false);
            if (snapshotDfnData == null)
            {
                snapshotDfnData = new SnapshotDefinitionData(
                    snapshotDfnUuid,
                    rscDfn,
                    snapshotName,
                    StateFlagsBits.getMask(flags),
                    driver,
                    transObjFactory,
                    transMgrProvider,
                    new TreeMap<>(),
                    new TreeMap<>()
                );
                rscDfn.addSnapshotDfn(accCtx, snapshotDfnData);
            }
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return snapshotDfnData;
    }
}
