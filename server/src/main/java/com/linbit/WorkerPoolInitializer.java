package com.linbit;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.MathUtils;

public class WorkerPoolInitializer
{
    // ============================================================
    // Worker thread pool defaults
    //

    // Minimum size of the work queue
    private static final int DFLT_MIN_WORKQ_SIZE = 32;

    // Minimum number of worker threads
    private static final int MIN_WORKER_COUNT = 4;

    // Maximum number of CPUs for calculations based on CPU count
    private static final int MAX_CPU_COUNT = 1024;

    // Default factor for the work queue size
    private static final int DFLT_WORKQ_FACTOR = 4;

    // ============================================================
    // Satellite device manager worker thread pool settings
    //

    // Minimum size of the work queue
    private static final int STLT_MIN_WORKQ_SIZE = 8;

    // Maximum number of worker threads for the Satellite module's device manager
    private static final int MAX_STLT_WORKER_COUNT = 12;

    // Queue size factor for the Satellite module device manager's work queue size
    private static final int STLT_WORKQ_FACTOR = 2;

    public static WorkerPool createDefaultWorkerThreadPool(
        ErrorReporter errorLog,
        ControllerDatabase controllerDatabase,
        String namePrefix
    )
    {
        int cpuCount = LinStor.CPU_COUNT;
        int thrCount = MathUtils.bounds(MIN_WORKER_COUNT, cpuCount, MAX_CPU_COUNT);
        int qSize = thrCount * DFLT_WORKQ_FACTOR;
        qSize = qSize > DFLT_MIN_WORKQ_SIZE ? qSize : DFLT_MIN_WORKQ_SIZE;
        return WorkerPool.initialize(
            thrCount, qSize, true, namePrefix, errorLog,
            controllerDatabase
        );
    }

    public static WorkerPool createDevMgrWorkerThreadPool(
        ErrorReporter errorLog,
        ControllerDatabase controllerDatabase,
        String namePrefix
    )
    {
        int cpuCount = LinStor.CPU_COUNT;
        int thrCount = MathUtils.bounds(MIN_WORKER_COUNT, cpuCount, MAX_STLT_WORKER_COUNT);
        int qSize = thrCount * STLT_WORKQ_FACTOR;
        qSize = qSize > STLT_MIN_WORKQ_SIZE ? qSize : STLT_MIN_WORKQ_SIZE;
        return WorkerPool.initialize(
            thrCount, qSize, true, namePrefix, errorLog,
            controllerDatabase
        );
    }
}
