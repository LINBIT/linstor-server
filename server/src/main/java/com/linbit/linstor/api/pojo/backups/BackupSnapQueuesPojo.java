package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;

public class BackupSnapQueuesPojo
{
    private final String resourceName;
    private final String snapshotName;
    private final String remoteName;
    private final boolean incremental;
    private final @Nullable String basedOn;
    private final @Nullable Long startTimestamp;
    private final @Nullable String prefNode;
    /**
     * The list of nodes this snapshot is queued on. Will be empty if this
     * is an item of BackupNodeQueuesPojo.queue
     */
    private final @Nullable List<BackupNodeQueuesPojo> queue;

    public BackupSnapQueuesPojo(
        String resourceNameRef,
        String snapshotNameRef,
        String remoteNameRef,
        boolean incrementalRef,
        @Nullable String basedOnRef,
        @Nullable Long startTimestampRef,
        @Nullable String prefNodeRef,
        @Nullable List<BackupNodeQueuesPojo> queueRef
    )
    {
        resourceName = resourceNameRef;
        snapshotName = snapshotNameRef;
        remoteName = remoteNameRef;
        incremental = incrementalRef;
        basedOn = basedOnRef;
        startTimestamp = startTimestampRef;
        prefNode = prefNodeRef;
        queue = queueRef;
    }

    public String getResourceName()
    {
        return resourceName;
    }

    public String getSnapshotName()
    {
        return snapshotName;
    }

    public String getRemoteName()
    {
        return remoteName;
    }

    public boolean isIncremental()
    {
        return incremental;
    }

    public @Nullable String getBasedOn()
    {
        return basedOn;
    }

    public @Nullable Long getStartTimestamp()
    {
        return startTimestamp;
    }

    public @Nullable String getPrefNode()
    {
        return prefNode;
    }

    public @Nullable List<BackupNodeQueuesPojo> getQueue()
    {
        return queue;
    }
}
