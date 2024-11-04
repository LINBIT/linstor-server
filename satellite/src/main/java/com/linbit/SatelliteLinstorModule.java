package com.linbit;

import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class SatelliteLinstorModule extends AbstractModule
{
    // Name for worker pool for satellite services operations - DeviceManager, etc.
    public static final String STLT_WORKER_POOL_NAME = "StltWorkerPool";

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    // @Named(STLT_WORKER_POOL_NAME)
    public WorkerPool initializeStltWorkerThreadPool(ErrorReporter errorLog)
    {
        return WorkerPoolInitializer.createDevMgrWorkerThreadPool(
            errorLog,
            null,
            "StltWorkerPool"
        );
    }

    // @Provides
    // @Singleton
    // @Named(STLT_WORKER_POOL_NAME)
    // public WorkQueue initializeStltWorkerThreadQueue(WorkerPool pool)
    // {
    // return pool;
    // }
}
