package com.linbit;

import com.linbit.drbdmanage.logging.ErrorReporter;
import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerPool implements WorkQueue
{
    private final ArrayDeque<Runnable> workQueue;
    private final Semaphore workQueueGuard;

    private Thread[] workerList;
    private boolean terminate;

    private AtomicInteger unfinishedTasks;

    private int workQueueSize;
    private final String threadNamePrefix;

    private ErrorReporter errorLog;

    private WorkerPool(
        int parallelism,
        int queueSize,
        boolean fair,
        String namePrefix,
        ErrorReporter errorLogRef
    )
    {
        workQueue = new ArrayDeque<>(queueSize);
        workQueueGuard = new Semaphore(queueSize, fair);
        workerList = new Thread[parallelism];
        terminate = false;
        unfinishedTasks = new AtomicInteger();
        workQueueSize = queueSize;
        threadNamePrefix = namePrefix;
        errorLog = errorLogRef;
    }

    public static WorkerPool initialize(
        int parallelism,
        int queueSize,
        boolean fair,
        String namePrefix,
        ErrorReporter errorLogRef
    )
    {
        WorkerPool pool = new WorkerPool(parallelism, queueSize, fair, namePrefix, errorLogRef);

        for (int threadIndex = 0; threadIndex < parallelism; ++threadIndex)
        {
            WorkerThread worker = new WorkerThread(pool);
            worker.setName(String.format("%s_%04d", namePrefix, threadIndex));
            pool.workerList[threadIndex] = worker;
            worker.start();
        }

        return pool;
    }

    @Override
    public void submit(Runnable task)
    {
        workQueueGuard.acquireUninterruptibly();
        synchronized (workQueue)
        {
            if (workQueue.offerLast(task))
            {
                unfinishedTasks.incrementAndGet();
                workQueue.notify();
            }
            else
            {
                workQueueGuard.release();
            }
        }
    }

    public void finish()
    {
        synchronized (this)
        {
            while (!terminate && unfinishedTasks.get() != 0)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException interrupt)
                {
                }
            }
        }
    }

    public void shutdown()
    {
        terminate = true;
        synchronized (workQueue)
        {
            workQueue.notifyAll();
        }
        synchronized (this)
        {
            notifyAll();
        }
    }

    private Runnable next()
    {
        Runnable task = null;
        synchronized (workQueue)
        {
            while (!terminate)
            {
                task = workQueue.pollFirst();
                if (task == null)
                {
                    try
                    {
                        workQueue.wait();
                    }
                    catch (InterruptedException interrupt)
                    {
                    }
                }
                else
                {
                    break;
                }
            }
        }
        return task;
    }

    public int getThreadCount()
    {
        return workerList.length;
    }

    public int getQueueSize()
    {
        return workQueueSize;
    }

    public boolean isFairQueue()
    {
        return workQueueGuard.isFair();
    }

    private static class WorkerThread extends Thread
    {
        WorkerPool pool;

        WorkerThread(WorkerPool poolRef)
        {
            pool = poolRef;
        }

        @Override
        public void run()
        {
            Runnable task;
            do
            {
                task = pool.next();
                if (task != null)
                {
                    pool.workQueueGuard.release();
                    try
                    {
                        task.run();
                    }
                    catch (Exception exc)
                    {
                        pool.errorLog.reportError(exc);
                    }
                    catch (ImplementationError implError)
                    {
                        pool.errorLog.reportError(implError);
                    }
                    if (pool.unfinishedTasks.decrementAndGet() == 0)
                    {
                        synchronized (pool)
                        {
                            pool.notifyAll();
                        }
                    }
                }
            }
            while (task != null);
        }
    }
}
