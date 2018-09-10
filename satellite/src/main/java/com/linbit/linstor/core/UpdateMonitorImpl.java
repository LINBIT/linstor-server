package com.linbit.linstor.core;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

@Singleton
public class UpdateMonitorImpl implements UpdateMonitor
{
    private final ReadWriteLock reconfigurationLock;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ReadWriteLock storPoolDfnMapLock;

    private final AtomicLong fullSyncId;
    private boolean currentFullSyncApplied = false;

    private final AtomicLong awaitedUpdateId;
    private final List<Object> waitObjects = new ArrayList<>();

    @Inject
    public UpdateMonitorImpl(
        @Named(CoreModule.RECONFIGURATION_LOCK)  ReadWriteLock reconfigurationLockRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        @Named(CoreModule.STOR_POOL_DFN_MAP_LOCK) ReadWriteLock storPoolDfnMapLockRef
    )
    {
        reconfigurationLock = reconfigurationLockRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        storPoolDfnMapLock = storPoolDfnMapLockRef;

        // Don't start with 0 to ensure the controller mirrors our fullSyncId
        fullSyncId = new AtomicLong(2);

        awaitedUpdateId = new AtomicLong(0);
    }

    @Override
    public long getCurrentFullSyncId()
    {
        return fullSyncId.get();
    }

    @Override
    public long getCurrentAwaitedUpdateId()
    {
        return awaitedUpdateId.get();
    }

    @Override
    public void awaitedUpdateApplied()
    {
        awaitedUpdateId.incrementAndGet();
    }

    @Override
    public long getNextFullSyncId()
    {
        long nextFullSyncId;
        try
        {
            reconfigurationLock.writeLock().lock();
            nodesMapLock.writeLock().lock();
            rscDfnMapLock.writeLock().lock();
            storPoolDfnMapLock.writeLock().lock();

            nextFullSyncId = fullSyncId.incrementAndGet();

            awaitedUpdateId.set(0);
            currentFullSyncApplied = false;
        }
        finally
        {
            storPoolDfnMapLock.writeLock().unlock();
            rscDfnMapLock.writeLock().unlock();
            nodesMapLock.writeLock().unlock();
            reconfigurationLock.writeLock().unlock();
        }
        return nextFullSyncId;
    }

    @Override
    public void setFullSyncApplied()
    {
        currentFullSyncApplied = true;
        for (Object waitObject : waitObjects)
        {
            synchronized (waitObject)
            {
                waitObject.notifyAll();
            }
        }
    }

    @Override
    public boolean isCurrentFullSyncApplied()
    {
        return currentFullSyncApplied;
    }

    @Override
    public void waitUntilCurrentFullSyncApplied(Object waitObject) throws InterruptedException
    {
        if (!currentFullSyncApplied)
        {
            synchronized (waitObject)
            {
                if (!currentFullSyncApplied)
                {
                    waitObjects.add(waitObject);
                    waitObject.wait();
                }
            }
        }

    }
}
