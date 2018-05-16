package com.linbit.linstor.core;

import com.linbit.linstor.SnapshotName;

public class SnapshotState
{
    private final SnapshotName snapshotName;

    private final boolean suspended;

    private final boolean snapshotTaken;

    public SnapshotState(SnapshotName snapshotNameRef, boolean suspendedRef, boolean snapshotTakenRef)
    {
        snapshotName = snapshotNameRef;
        suspended = suspendedRef;
        snapshotTaken = snapshotTakenRef;
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
}
