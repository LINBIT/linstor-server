package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.linbit.linstor.logging.ErrorReporter;
import javax.inject.Named;

import javax.inject.Singleton;

public class SatelliteLinstorModule extends AbstractModule
{
    public static final String MAIN_WORKER_POOL_NAME = "MainWorkerPool";
    public static final String STLT_WORKER_POOL_NAME = "StltWorkerPool";

    @Override
    protected void configure()
    {
        bind(WorkQueue.class).annotatedWith(Names.named(MAIN_WORKER_POOL_NAME))
            .to(Key.get(WorkerPool.class, Names.named(MAIN_WORKER_POOL_NAME)));
        bind(WorkQueue.class).annotatedWith(Names.named(STLT_WORKER_POOL_NAME))
            .to(Key.get(WorkerPool.class, Names.named(STLT_WORKER_POOL_NAME)));
    }

    @Provides
    @Singleton
    @Named(MAIN_WORKER_POOL_NAME)
    public WorkerPool initializeMainWorkerThreadPool(ErrorReporter errorLog)
    {
        return WorkerPoolInitializer.createDefaultWorkerThreadPool(
            errorLog,
            null,
            "MainWorkerPool"
        );
    }

    @Provides
    @Singleton
    @Named(STLT_WORKER_POOL_NAME)
    public WorkerPool initializeStltWorkerThreadPool(ErrorReporter errorLog)
    {
        return WorkerPoolInitializer.createDefaultWorkerThreadPool(
            errorLog,
            null,
            "StltWorkerPool"
        );
    }
}
