package com.linbit;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

public class WorkerPoolTest
{
    private static final int DEFAULT_THREAD_COUNT = 3;
    private static final int DEFAULT_QUEUE_SIZE = 10;
    private static final boolean DEFAULT_FAIRNESS = true;
    private static final String DEFAULT_THREAD_PREFIX = "TestWorkerThread";
    private static final TestErrorReporter DEFAULT_ERROR_REPORTER = new TestErrorReporter();

    private WorkerPool pool;

    @Before
    public void setUp() throws Exception
    {
    }

    @After
    public void tearDown() throws Exception
    {
        if (pool != null)
        {
            pool.shutdown();
        }
    }

    @Test
    public void testCreatedThreadAmount()
    {
        pool = WorkerPoolBuilder.create().build();

        Assert.assertEquals("Unexpected worker count", DEFAULT_THREAD_COUNT, pool.getThreadCount());
    }

    @Test
    public void testQueueSize()
    {
        pool = WorkerPoolBuilder.create().build();

        Assert.assertEquals("Unexpected worker count", DEFAULT_QUEUE_SIZE, pool.getQueueSize());
    }

    @Test
    public void testFairness()
    {
        pool = WorkerPoolBuilder.create().build();

        Assert.assertEquals("Unexpected fairness", DEFAULT_FAIRNESS, pool.isFairQueue());
    }

    @Test
    public void testThreadPrefix()
    {
        pool = WorkerPoolBuilder.create().build();

        int prefixedThreadCount = getPrefixedThreadCount();
        Assert.assertEquals("Unexpected prefixed thread count", prefixedThreadCount, DEFAULT_THREAD_COUNT);
    }

    @Test
    public void testShutdown() throws InterruptedException, ExecutionException, TimeoutException
    {
        pool = WorkerPoolBuilder.create().build();

        int prefixedThreadCount = getPrefixedThreadCount();
        Assert.assertEquals("Unexpected prefixed thread count", prefixedThreadCount, DEFAULT_THREAD_COUNT);

        pool.shutdown();

        waitUntilPoolFinishes();

        prefixedThreadCount = getPrefixedThreadCount();
        for (int waitTimes = 0; prefixedThreadCount > 0 && waitTimes < 10; waitTimes++)
        {
            // wait a little longer (max 1 sec)
            Thread.sleep(100);
            prefixedThreadCount = getPrefixedThreadCount();
        }
        Assert.assertEquals("Worker threads still running", prefixedThreadCount, 0);
    }


    @Test
    public void testSumbitSimpleTask() throws InterruptedException, ExecutionException, TimeoutException
    {
        pool = WorkerPoolBuilder.create().build();

        final AtomicInteger finishedTasks = new AtomicInteger(0);

        Runnable task = new Runnable()
            {
                @Override
                public void run()
                {
                    finishedTasks.incrementAndGet();
                }
            };
        final int taskCount = DEFAULT_QUEUE_SIZE;
        for (int i = 0; i < taskCount; i++)
        {
            pool.submit(task);
        }

        waitUntilPoolFinishes();

        Assert.assertEquals("Not all tasks were executed", finishedTasks.get(), taskCount);
    }

    @Test
    public void testSubmitTaskWithException() throws InterruptedException
    {
        TestErrorReporter errorReporter = new TestErrorReporter();
        pool = WorkerPoolBuilder.create().errorReporter(errorReporter).build();

        final int exceptionId = 1;
        Runnable taks = new Runnable()
        {
            @Override
            public void run()
            {
                throw new TestException(exceptionId);
            }
        };

        pool.submit(taks);

        final Throwable throwable = errorReporter.unexpected.poll(10, TimeUnit.SECONDS);
        Assert.assertEquals("Unexpected throwable received", throwable.getClass(), TestException.class);
        Assert.assertEquals("Unexpected exception id received", ((TestException)throwable).id, exceptionId);
    }

    @Test
    public void testSubmitTaskWithImplementationError() throws InterruptedException
    {
        TestErrorReporter errorReporter = new TestErrorReporter();
        pool = WorkerPoolBuilder.create().errorReporter(errorReporter).build();

        final int exceptionId = 1;
        Runnable taks = new Runnable()
        {
            @Override
            public void run()
            {
                throw new ImplementationError(new TestException(exceptionId));
            }
        };

        pool.submit(taks);

        final Throwable throwable = errorReporter.unexpected.poll(10, TimeUnit.SECONDS);
        Assert.assertEquals("Unexpected throwable received", throwable.getClass(), ImplementationError.class);
        final Throwable cause = throwable.getCause();
        Assert.assertEquals("Unexpected cause received", cause.getClass(), TestException.class);
        Assert.assertEquals("Unexpected exception id received", ((TestException)cause).id, exceptionId);
    }

    private int getPrefixedThreadCount()
    {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        int prefixedThreadCount = 0;
        for (Thread thread : threads)
        {
            if (thread.getName().startsWith(DEFAULT_THREAD_PREFIX))
            {
                prefixedThreadCount++;
            }
        }
        return prefixedThreadCount;
    }


    private void waitUntilPoolFinishes() throws InterruptedException, ExecutionException, TimeoutException
    {
        exec(new Runnable()
            {
                @Override
                public void run()
                {
                    pool.finish();
                }
            },
            15_000);
    }


    private void exec(Runnable task, long millisec)
        throws InterruptedException, ExecutionException, TimeoutException
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(task).get(millisec, TimeUnit.MILLISECONDS);
        executor.shutdown();
    }

    private static class TestErrorReporter implements ErrorReporter
    {
        private BlockingQueue<Throwable> unexpected = new LinkedBlockingQueue<>();
        // private BlockingQueue<Throwable> expected = new LinkedBlockingQueue<>();

        @Override
        public void reportError(Throwable throwable)
        {
            unexpected.add(throwable);
        }

        @Override
        public void reportError(Throwable errorInfo, AccessContext accCtx, Peer client, String contextInfo)
        {
            unexpected.add(errorInfo);
        }

        @Override
        public void reportProblem(
            Level logLevel,
            DrbdManageException errorInfo,
            AccessContext accCtx,
            Peer client,
            String contextInfo
        )
        {
            unexpected.add(errorInfo);
        }
    }

    private static class WorkerPoolBuilder
    {
        private int parallelism = DEFAULT_THREAD_COUNT;
        private int queueSize = DEFAULT_QUEUE_SIZE;
        private boolean fairness = DEFAULT_FAIRNESS;
        private String threadPrefix = DEFAULT_THREAD_PREFIX;
        private ErrorReporter errorReporter = DEFAULT_ERROR_REPORTER;

        private WorkerPoolBuilder()
        {
        }

        public static WorkerPoolBuilder create()
        {
            return new WorkerPoolBuilder();
        }

        public WorkerPool build()
        {
            return WorkerPool.initialize(parallelism, queueSize, fairness, threadPrefix, errorReporter);
        }

        public WorkerPoolBuilder parallelism(int parallelism)
        {
            this.parallelism = parallelism;
            return this;
        }
        public WorkerPoolBuilder queueSize(int queueSize)
        {
            this.queueSize = queueSize;
            return this;
        }
        public WorkerPoolBuilder fair(boolean fairness)
        {
            this.fairness = fairness;
            return this;
        }
        public WorkerPoolBuilder threadPrefix(String prefix)
        {
            threadPrefix = prefix;
            return this;
        }
        public WorkerPoolBuilder errorReporter(ErrorReporter reporter)
        {
            errorReporter = reporter;
            return this;
        }
    }

    private static class TestException extends RuntimeException
    {
        private static final long serialVersionUID = 1308902008629518167L;

        public int id;

        TestException(int id)
        {
            super(Integer.toString(id));
            this.id = id;
        }
    }
}
