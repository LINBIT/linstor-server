package com.linbit.linstor.core;

public interface UpdateMonitor
{
    long getCurrentFullSyncId();

    long getCurrentAwaitedUpdateId();

    void awaitedUpdateApplied();

    long getNextFullSyncId();

    void setFullSyncApplied();

    boolean isCurrentFullSyncApplied();

    void waitUntilCurrentFullSyncApplied(Object waitObject) throws InterruptedException;
}
