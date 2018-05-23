package com.linbit.linstor;

import java.util.Collection;
import java.util.UUID;

public interface SnapshotDefinition
{
    UUID getUuid();

    ResourceDefinition getResourceDefinition();

    SnapshotName getName();

    Snapshot getSnapshot(NodeName clNodeName);

    Collection<Snapshot> getAllSnapshots();

    void addSnapshot(Snapshot snapshotRef);

    void removeSnapshot(Snapshot snapshotRef);

    boolean isSuccessfullyTaken();

    void setSuccessfullyTaken(boolean successfullyTakenRef);

    boolean isFailedDueToDisconnect();

    void setFailedDueToDisconnect(boolean failedDueToDisconnectRef);

    UUID debugGetVolatileUuid();
}
