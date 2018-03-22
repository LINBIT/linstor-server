package com.linbit.utils;

import java.util.concurrent.locks.Lock;

public class LockSupport implements AutoCloseable
{
    private Lock[] locks;

    public LockSupport(Lock... locksRef)
    {
        locks = locksRef;
    }

    @Override
    public void close()
    {
        for (int idx = locks.length - 1; idx >= 0; idx--)
        {
            locks[idx].unlock();
        }
    }

    public static LockSupport lock(Lock...locks)
    {
        for (Lock lock : locks)
        {
            lock.lock();
        }
        return new LockSupport(locks);
    }
}
