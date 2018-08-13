package com.linbit.linstor.core;

public class SnapshotState
{
    private final boolean suspended;

    private final boolean snapshotTaken;

    private final boolean snapshotDeleted;

    public SnapshotState(
        boolean suspendedRef,
        boolean snapshotTakenRef,
        boolean snapshotDeletedRef
    )
    {
        suspended = suspendedRef;
        snapshotTaken = snapshotTakenRef;
        snapshotDeleted = snapshotDeletedRef;
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
