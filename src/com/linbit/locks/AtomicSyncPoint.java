package com.linbit.locks;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class AtomicSyncPoint implements SyncPoint
{
    private AtomicLong count;

    public AtomicSyncPoint()
    {
        count = new AtomicLong(0L);
    }

    @Override
    public void register()
    {
        count.incrementAndGet();
    }

    @Override
    public void arrive()
    {
        long value = count.decrementAndGet();
        if (value <= 0)
        {
            if (value < 0)
            {
                // This can only happen whenever arrive() is called more than once per register(),
                // which is an external implementation error
                count.set(0L);
            }
            synchronized (this)
            {
                notifyAll();
            }
        }
    }

    @Override
    public void await()
    {
        synchronized (this)
        {
            while (count.get() > 0)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException ignored)
                {
                }
            }
        }
    }
}
