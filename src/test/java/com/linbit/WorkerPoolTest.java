package com.linbit;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;

import java.nio.file.Path;
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
        pool = new WorkerPoolBuilder().build();

        Assert.assertEquals("Unexpected worker count", DEFAULT_THREAD_COUNT, pool.getThreadCount());
    }

    @Test
    public void testQueueSize()
    {
        pool = new WorkerPoolBuilder().build();

        Assert.assertEquals("Unexpected worker count", DEFAULT_QUEUE_SIZE, pool.getQueueSize());
    }

    @Test
    public void testFairness()
    {
        pool = new WorkerPoolBuilder().build();

        Assert.assertEquals("Unexpected fairness", DEFAULT_FAIRNESS, pool.isFairQueue());
    }

    @Test
    public void testThreadPrefix()
    {
        WorkerPoolBuilder workerPoolBuilder = new WorkerPoolBuilder();
        pool = workerPoolBuilder.build();

        int prefixedThreadCount = getPrefixedThreadCount(workerPoolBuilder.threadPrefix);
        Assert.assertEquals("Unexpected prefixed thread count", prefixedThreadCount, DEFAULT_THREAD_COUNT);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testShutdown() throws InterruptedException, ExecutionException, TimeoutException
    {
        WorkerPoolBuilder workerPoolBuilder = new WorkerPoolBuilder();
        pool = workerPoolBuilder.build();

        int prefixedThreadCount = getPrefixedThreadCount(workerPoolBuilder.threadPrefix);
        Assert.assertEquals("Unexpected prefixed thread count", prefixedThreadCount, DEFAULT_THREAD_COUNT);

        pool.shutdown();

        waitUntilPoolFinishes();

        prefixedThreadCount = getPrefixedThreadCount(workerPoolBuilder.threadPrefix);
        for (int waitTimes = 0; prefixedThreadCount > 0 && waitTimes < 10; waitTimes++)
        {
            // wait a little longer (max 1 sec)
            Thread.sleep(100);
            prefixedThreadCount = getPrefixedThreadCount(workerPoolBuilder.threadPrefix);
        }
        Assert.assertEquals("Worker threads still running", prefixedThreadCount, 0);
    }

    @Test
    public void testSumbitSimpleTask() throws InterruptedException, ExecutionException, TimeoutException
    {
        pool = new WorkerPoolBuilder().build();

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
        for (int idx = 0; idx < taskCount; idx++)
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
        pool = new WorkerPoolBuilder().errorReporter(errorReporter).build();

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
        Assert.assertEquals("Unexpected exception id received", ((TestException) throwable).id, exceptionId);
    }

    @Test
    public void testSubmitTaskWithImplementationError() throws InterruptedException
    {
        TestErrorReporter errorReporter = new TestErrorReporter();
        pool = new WorkerPoolBuilder().errorReporter(errorReporter).build();

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
        Assert.assertEquals("Unexpected exception id received", ((TestException) cause).id, exceptionId);
    }

    private int getPrefixedThreadCount(String threadPrefix)
    {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        int prefixedThreadCount = 0;
        for (Thread thread : threads)
        {
            if (thread.getName().startsWith(threadPrefix))
            {
                prefixedThreadCount++;
            }
        }
        return prefixedThreadCount;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private void waitUntilPoolFinishes() throws InterruptedException, ExecutionException, TimeoutException
    {
        exec(
            new Runnable()
            {
                @Override
                public void run()
                {
                    if (pool == null)
                    {
                        throw new ImplementationError("no pool to wait for");
                    }
                    pool.finish();
                }
            },
            15_000
        );
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

        @Override
        public String getInstanceId()
        {
            // Hex instance ID of linstor's error reporter
            // Not significant for the test, just needs to return something to implement the interface
            return "CAFEAFFE";
        }

        @Override
        public String reportError(Throwable throwable)
        {
            unexpected.add(throwable);
            return null; // no error report, no logName
        }

        @Override
        public String reportError(Level logLevel, Throwable errorInfo)
        {
            unexpected.add(errorInfo);
            return null; // no error report, no logName
        }

        @Override
        public String reportError(
            Level logLevel,
            Throwable errorInfo,
            AccessContext accCtx,
            Peer client,
            String contextInfo
        )
        {
            unexpected.add(errorInfo);
            return null; // no error report, no logName
        }

        @Override
        public String reportError(
            Throwable errorInfo,
            AccessContext accCtx,
            Peer client,
            String contextInfo
        )
        {
            unexpected.add(errorInfo);
            return null; // no error report, no logName
        }

        @Override
        public String reportProblem(
            Level logLevel,
            LinStorException errorInfo,
            AccessContext accCtx,
            Peer client,
            String contextInfo
        )
        {
            unexpected.add(errorInfo);
            return null; // no error report, no logName
        }

        @Override
        public void logTrace(String format, Object... args)
        {
            log("TRACE", format, args);
        }

        @Override
        public void logDebug(String format, Object... args)
        {
            log("DEBUG", format, args);
        }

        @Override
        public void logInfo(String format, Object... args)
        {
            log("INFO ", format, args);
        }

        @Override
        public void logWarning(String format, Object... args)
        {
            log("WARN ", format, args);
        }

        @Override
        public void logError(String format, Object... args)
        {
            log("ERROR", format, args);
        }

        private void log(String type, String format, Object[] args)
        {
            System.err.printf(
                "%s %s\\n",
                type,
                String.format(
                    format,
                    args
                )
            );
        }

        @Override
        public void setLogLevel(@Nullable AccessContext accCtx, @Nullable Level level, @Nullable Level linstorLevel)
        {
            // Tracing on/off not implemented, no-op
        }

        @Override
        public boolean hasAtLeastLogLevel(Level leveRef)
        {
            return true;
        }

        @Override
        public Level getCurrentLogLevel()
        {
            return Level.TRACE;
        }

        @Override
        public Path getLogDirectory()
        {
            return null;
        }
    }

    private static class WorkerPoolBuilder
    {
        private static final AtomicInteger ID_GEN = new AtomicInteger(0);

        private int parallelism = DEFAULT_THREAD_COUNT;
        private int queueSize = DEFAULT_QUEUE_SIZE;
        private boolean fairness = DEFAULT_FAIRNESS;
        private String threadPrefix;
        private ErrorReporter errorReporter = DEFAULT_ERROR_REPORTER;

        private WorkerPoolBuilder()
        {
            threadPrefix = DEFAULT_THREAD_PREFIX + "_" + Integer.toString(ID_GEN.incrementAndGet());
        }

        public WorkerPool build()
        {
            return WorkerPool.initialize(parallelism, queueSize, fairness, threadPrefix, errorReporter, null);
        }

        public WorkerPoolBuilder parallelism(int parallelismRef)
        {
            parallelism = parallelismRef;
            return this;
        }

        public WorkerPoolBuilder queueSize(int queueSizeRef)
        {
            queueSize = queueSizeRef;
            return this;
        }

        public WorkerPoolBuilder fair(boolean fairnessRef)
        {
            fairness = fairnessRef;
            return this;
        }

        public WorkerPoolBuilder threadPrefix(String prefixRef)
        {
            threadPrefix = prefixRef;
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

        TestException(int idRef)
        {
            super(Integer.toString(idRef));
            id = idRef;
        }
    }
}
