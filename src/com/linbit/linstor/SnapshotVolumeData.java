package com.linbit.linstor;

import com.linbit.linstor.api.pojo.SnapshotVlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDataDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.sql.SQLException;
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

    private final SnapshotVolumeDataDatabaseDriver dbDriver;

    private final TransactionSimpleObject<SnapshotVolumeData, Boolean> deleted;

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
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            snapshot,
            snapshotVolumeDefinition,
            deleted
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
        checkDeleted();
        return snapshot;
    }

    @Override
    public SnapshotVolumeDefinition getSnapshotVolumeDefinition()
    {
        checkDeleted();
        return snapshotVolumeDefinition;
    }

    @Override
    public StorPool getStorPool(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return storPool;
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        if (!deleted.get())
        {
            snapshot.getSnapshotDefinition().getResourceDefinition()
                .getObjProt().requireAccess(accCtx, AccessType.CONTROL);

            snapshot.removeSnapshotVolume(this);
            snapshotVolumeDefinition.removeSnapshotVolume(this);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot volume");
        }
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
