package com.linbit.locks;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * A group of locks which will be locked in the given order and unlocked in the reverse order.
 */
public class MultiLock
{
    private final List<Lock> locks;

    public MultiLock(final List<Lock> locks)
    {
        this.locks = locks;
    }

    public void lock()
    {
        for (Lock lock : locks)
        {
            lock.lock();
        }
    }

    public void unlock()
    {
        for (int index = locks.size() - 1; index >= 0; index--)
        {
            locks.get(index).unlock();
        }
    }
}
