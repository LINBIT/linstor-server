package com.linbit.linstor;

import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class SnapshotVolumeDefinitionData extends BaseTransactionObject implements SnapshotVolumeDefinition
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    private final SnapshotDefinition snapshotDfn;

    // DRBD volume number
    private final VolumeNumber volumeNr;

    private final SnapshotVolumeDefinitionDatabaseDriver dbDriver;

    private final TransactionMap<NodeName, SnapshotVolume> snapshotVlmMap;

    private final TransactionSimpleObject<SnapshotVolumeDefinitionData, Boolean> deleted;

    public SnapshotVolumeDefinitionData(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        VolumeNumber volNr,
        SnapshotVolumeDefinitionDatabaseDriver dbDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProviderRef,
        Map<NodeName, SnapshotVolume> snapshotVlmMapRef
    )
    {
        super(transMgrProviderRef);

        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        volumeNr = volNr;
        dbDriver = dbDriverRef;

        dbgInstanceId = UUID.randomUUID();

        snapshotVlmMap = transObjFactory.createTransactionMap(snapshotVlmMapRef, null);

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            snapshotDfn,
            snapshotVlmMap,
            deleted
        );
    }

    @Override
    public UUID getUuid()
    {
        return objId;
    }

    @Override
    public SnapshotDefinition getSnapshotDefinition()
    {
        checkDeleted();
        return snapshotDfn;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        checkDeleted();
        return volumeNr;
    }

    @Override
    public void addSnapshotVolume(SnapshotVolume snapshotVolume)
    {
        checkDeleted();
        snapshotVlmMap.put(snapshotVolume.getSnapshot().getNode().getName(), snapshotVolume);
    }

    @Override
    public void removeSnapshotVolume(SnapshotVolumeData snapshotVolumeData)
    {
        checkDeleted();
        snapshotVlmMap.remove(snapshotVolumeData.getSnapshot().getNode().getName());
    }

    @Override
    public void delete(AccessContext accCtx)
        throws SQLException
    {
        if (!deleted.get())
        {
            snapshotDfn.removeSnapshotVolumeDefinition(volumeNr);

            activateTransMgr();
            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted snapshot volume definition");
        }
    }

    @Override
    public String toString()
    {
        return snapshotDfn + ", VlmNr: '" + volumeNr + "'";
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    @Override
    public SnapshotVlmDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException
    {
        return new SnapshotVlmDfnPojo(
            getUuid(),
            getVolumeNumber().value
        );
    }
}
