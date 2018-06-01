package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.UUID;

public interface SnapshotVolumeDefinition extends TransactionObject
{
    UUID getUuid();

    SnapshotDefinition getSnapshotDefinition();

    VolumeNumber getVolumeNumber();

    UUID debugGetVolatileUuid();

    SnapshotVlmDfnApi getApiData(AccessContext accCtx)
        throws AccessDeniedException;

    public interface SnapshotVlmDfnApi
    {
        UUID getUuid();
        Integer getVolumeNr();
    }
}
