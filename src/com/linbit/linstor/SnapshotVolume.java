package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public interface SnapshotVolume extends TransactionObject, DbgInstanceUuid
{
    UUID getUuid();

    Snapshot getSnapshot();

    SnapshotVolumeDefinition getSnapshotVolumeDefinition();

    StorPool getStorPool(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    SnapshotVlmApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    interface SnapshotVlmApi
    {
        UUID getSnapshotVlmUuid();
        UUID getSnapshotVlmDfnUuid();
        String getStorPoolName();
        UUID getStorPoolUuid();
        int getSnapshotVlmNr();
    }
}
