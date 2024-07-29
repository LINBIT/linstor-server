package com.linbit;

import com.linbit.linstor.ControllerDatabase;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.utils.MathUtils;

public class WorkerPoolInitializer
{
    // ============================================================
    // Satellite device manager worker thread pool settings
    //

    // Minimum size of the work queue
    private static final int STLT_MIN_WORKQ_SIZE = 8;

    // Minimum number of worker threads
    private static final int MIN_WORKER_COUNT = 4;

    // Maximum number of worker threads for the Satellite module's device manager
    private static final int MAX_STLT_WORKER_COUNT = 12;

    // Queue size factor for the Satellite module device manager's work queue size
    private static final int STLT_WORKQ_FACTOR = 2;

    public static WorkerPool createDevMgrWorkerThreadPool(
        ErrorReporter errorLog,
        @Nullable ControllerDatabase controllerDatabase,
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

    private WorkerPoolInitializer()
    {
    }
}
