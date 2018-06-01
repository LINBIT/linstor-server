package com.linbit.linstor;

import com.linbit.linstor.api.pojo.SnapshotVlmDfnPojo;
import com.linbit.linstor.dbdrivers.interfaces.SnapshotVolumeDefinitionDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;

import javax.inject.Provider;
import java.util.Arrays;
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

    public SnapshotVolumeDefinitionData(
        UUID objIdRef,
        SnapshotDefinition snapshotDfnRef,
        VolumeNumber volNr,
        SnapshotVolumeDefinitionDatabaseDriver snapshotVolumeDefinitionDatabaseDriverRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        objId = objIdRef;
        snapshotDfn = snapshotDfnRef;
        volumeNr = volNr;

        dbgInstanceId = UUID.randomUUID();

        transObjs = Arrays.asList(
            snapshotDfn
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
        return snapshotDfn;
    }

    @Override
    public VolumeNumber getVolumeNumber()
    {
        return volumeNr;
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
