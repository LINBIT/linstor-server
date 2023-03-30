package com.linbit.linstor.api.pojo.backups;

import javax.annotation.Nullable;

import java.util.List;

public class BackupSnapQueuesPojo
{
    private final String resourceName;
    private final String snapshotName;
    private final String remoteName;
    private final boolean incremental;
    private final String basedOn;
    private final Long startTimestamp;
    private final String prefNode;
    /**
     * The list of nodes this snapshot is queued on. Will be empty if this
     * is an item of BackupNodeQueuesPojo.queue
     */
    private final List<BackupNodeQueuesPojo> queue;

    public BackupSnapQueuesPojo(
        String resourceNameRef,
        String snapshotNameRef,
        String remoteNameRef,
        boolean incrementalRef,
        @Nullable String basedOnRef,
        @Nullable Long startTimestampRef,
        String prefNodeRef,
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

    public String getBasedOn()
    {
        return basedOn;
    }

    public Long getStartTimestamp()
    {
        return startTimestamp;
    }

    public String getPrefNode()
    {
        return prefNode;
    }

    public List<BackupNodeQueuesPojo> getQueue()
    {
        return queue;
    }
}
