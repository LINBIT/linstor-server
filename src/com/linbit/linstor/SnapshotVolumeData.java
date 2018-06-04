package com.linbit.linstor;

import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;
import java.util.Arrays;
import java.util.UUID;

public class SnapshotVolumeData extends BaseTransactionObject implements SnapshotVolume
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final Snapshot snapshot;

    private final SnapshotVolumeDefinition snapshotVolumeDefinition;

    private final StorPool storPool;

    public SnapshotVolumeData(
        UUID objIdRef,
        Snapshot snapshotRef,
        SnapshotVolumeDefinition snapshotVolumeDefinitionRef,
        StorPool storPoolRef,
        SnapshotVolumeDataDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        objId = objIdRef;
        snapshot = snapshotRef;
        snapshotVolumeDefinition = snapshotVolumeDefinitionRef;
        storPool = storPoolRef;

        dbgInstanceId = UUID.randomUUID();

        transObjs = Arrays.asList(
            snapshot,
            snapshotVolumeDefinition
        );
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public Snapshot getSnapshot()
    {
        return snapshot;
    }

    @Override
    public SnapshotVolumeDefinition getSnapshotVolumeDefinition()
    {
        return snapshotVolumeDefinition;
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx)
        throws AccessDeniedException
    {
        return storPool;
    }

    @Override
    public String toString()
    {
        return snapshot + ", VlmNr: '" + snapshotVolumeDefinition.getVolumeNumber() + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public SnapshotVlmApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmPojo(
            getStorPool(accCtx).getName().getDisplayName(),
            getStorPool(accCtx).getUuid(),
            getSnapshotVolumeDefinition().getUuid(),
            getUuid(),
            getSnapshotVolumeDefinition().getVolumeNumber().value
        );
    }
}
