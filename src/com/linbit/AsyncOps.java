package com.linbit;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Perform background operations asynchronously and wait for their completion
 * or the timeout
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class AsyncOps
{
    private final CountDownLatch syncPoint;

    private AsyncOps(CountDownLatch syncPointRef)
    {
        syncPoint = syncPointRef;
    }

    public void await()
    {
        try
        {
            syncPoint.await();
        }
        catch (InterruptedException intrExc)
        {
            // No-op
            // Allow intentional interruption to continue the
            // waiting thread immediately
        }
    }

    public void await(long timeout)
    {
        try
        {
            syncPoint.await(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException intrExc)
        {
            // No-op
            // Allow intentional interruption to continue the
            // waiting thread immediately
        }
    }

    public static class Builder
    {
        private int count;
        private List<Runnable> runList;

        public Builder()
        {
            count = 0;
            runList = new LinkedList<>();
        }

        public void register(Runnable op)
        {
            runList.add(op);
            ++count;
        }

        public AsyncOps create()
        {
            // Create the AsyncOps controller object
            CountDownLatch opSyncPoint = new CountDownLatch(count);
            AsyncOps opsObj = new AsyncOps(opSyncPoint);

            // Spawn the registered asynchronous operation threads
            for (Runnable op : runList)
            {
                OpThread opThr = new OpThread(opSyncPoint, op);
                opThr.start();
            }

            return opsObj;
        }
    }

    /**
     * Wrapper around a Runnable instance; notifies the synchronization point
     * when the Runnable instance returns
     */
    private static class OpThread extends Thread
    {
        private final Runnable operation;
        private final CountDownLatch opSyncPoint;

        OpThread(CountDownLatch syncPointRef, Runnable operationRef)
        {
            opSyncPoint = syncPointRef;
            operation = operationRef;
        }

        @Override
        public void run()
        {
            operation.run();
            opSyncPoint.countDown();
        }
    }
}
