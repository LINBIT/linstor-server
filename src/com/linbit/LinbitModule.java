package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.MathUtils;

public class LinbitModule extends AbstractModule
{
    // ============================================================
    // Worker thread pool defaults
    //
    public static final int MIN_WORKER_QUEUE_SIZE = 32;
    public static final int MIN_WORKER_COUNT      = 4;
    public static final int MAX_CPU_COUNT = 1024;

    private static final int WORKER_QUEUE_FACTOR = 4;

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    public WorkerPool initializeWorkerThreadPool(
        ErrorReporter errorLog,
        DbConnectionPool dbConnPool
    )
    {
        int cpuCount = Runtime.getRuntime().availableProcessors();
        int thrCount = MathUtils.bounds(MIN_WORKER_COUNT, cpuCount, MAX_CPU_COUNT);
        int qSize = thrCount * WORKER_QUEUE_FACTOR;
        qSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
        return WorkerPool.initialize(
            thrCount, qSize, true, "MainWorkerPool", errorLog,
            dbConnPool
        );
    }
}
