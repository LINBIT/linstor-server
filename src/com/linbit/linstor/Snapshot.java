package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.UUID;

public interface Snapshot
{
    UUID getUuid();

    SnapshotDefinition getSnapshotDefinition();

    Node getNode();

    boolean getSuspendResource();

    void setSuspendResource(boolean suspendResource);

    boolean getTakeSnapshot();

    void setTakeSnapshot(boolean takeSnapshot);

    UUID debugGetVolatileUuid();

    SnapshotApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException;

    public interface SnapshotApi
    {
        ResourceDefinition.RscDfnApi getRscDfn();
        UUID getSnapshotUuid();
        String getSnapshotName();
        UUID getSnapshotDfnUuid();
        boolean getSuspendResource();
        boolean getTakeSnapshot();
        Long getFullSyncId();
        Long getUpdateId();
    }
}
