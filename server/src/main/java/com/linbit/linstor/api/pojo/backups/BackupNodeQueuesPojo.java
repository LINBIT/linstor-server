package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.annotation.Nullable;

import java.util.List;

public class BackupNodeQueuesPojo
{
    private final String nodeName;
    /**
     * The list of queued snapshots. Will be empty if this is an item of
     * BackupSnapQueuesPojo.queue
     */
    private final @Nullable List<BackupSnapQueuesPojo> queue;

    public BackupNodeQueuesPojo(String nodeNameRef, @Nullable List<BackupSnapQueuesPojo> queueRef)
    {
        nodeName = nodeNameRef;
        queue = queueRef;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public List<BackupSnapQueuesPojo> getQueue()
    {
        return queue;
    }
}
