package com.linbit;

import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.MathUtils;

public class WorkerPoolInitializer
{
    // ============================================================
    // Worker thread pool defaults
    //
    private static final int MIN_WORKER_QUEUE_SIZE = 32;
    private static final int MIN_WORKER_COUNT      = 4;
    private static final int MAX_CPU_COUNT = 1024;

    private static final int WORKER_QUEUE_FACTOR = 4;

    public static WorkerPool createDefaultWorkerThreadPool(
        ErrorReporter errorLog,
        DbConnectionPool dbConnPool,
        String namePrefix
    )
    {
        int cpuCount = Runtime.getRuntime().availableProcessors();
        int thrCount = MathUtils.bounds(MIN_WORKER_COUNT, cpuCount, MAX_CPU_COUNT);
        int qSize = thrCount * WORKER_QUEUE_FACTOR;
        qSize = qSize > MIN_WORKER_QUEUE_SIZE ? qSize : MIN_WORKER_QUEUE_SIZE;
        return WorkerPool.initialize(
            thrCount, qSize, true, namePrefix, errorLog,
            dbConnPool
        );
    }
}
