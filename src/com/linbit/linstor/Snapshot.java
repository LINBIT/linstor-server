package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.UUID;

public interface Snapshot extends TransactionObject, DbgInstanceUuid
{
    UUID getUuid();

    SnapshotDefinition getSnapshotDefinition();

    Node getNode();

    boolean getSuspendResource();

    void setSuspendResource(boolean suspendResource);

    boolean getTakeSnapshot();

    void setTakeSnapshot(boolean takeSnapshot);

    SnapshotApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    public interface SnapshotApi
    {
        SnapshotDefinition.SnapshotDfnApi getSnaphotDfn();
        UUID getSnapshotUuid();
        boolean getSuspendResource();
        boolean getTakeSnapshot();
        Long getFullSyncId();
        Long getUpdateId();
    }
}
