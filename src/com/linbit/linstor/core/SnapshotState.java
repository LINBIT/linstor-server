package com.linbit.linstor.core;

import com.linbit.linstor.SnapshotName;

public class SnapshotState
{
    private final SnapshotName snapshotName;

    private final boolean suspended;

    private final boolean snapshotTaken;

    private final boolean snapshotDeleted;

    public SnapshotState(
        SnapshotName snapshotNameRef,
        boolean suspendedRef,
        boolean snapshotTakenRef,
        boolean snapshotDeletedRef
    )
    {
        snapshotName = snapshotNameRef;
        suspended = suspendedRef;
        snapshotTaken = snapshotTakenRef;
        snapshotDeleted = snapshotDeletedRef;
    }

    public SnapshotName getSnapshotName()
    {
        return snapshotName;
    }

    public boolean isSuspended()
    {
        return suspended;
    }

    public boolean isSnapshotTaken()
    {
        return snapshotTaken;
    }

    public boolean isSnapshotDeleted()
    {
        return snapshotDeleted;
    }
}
