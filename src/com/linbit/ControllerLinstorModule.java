package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Named;
import javax.inject.Singleton;

public class ControllerLinstorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(WorkQueue.class).annotatedWith(Names.named(LinStorModule.MAIN_WORKER_POOL_NAME))
            .to(Key.get(WorkerPool.class, Names.named(LinStorModule.MAIN_WORKER_POOL_NAME)));
    }

    @Provides
    @Singleton
    @Named(LinStorModule.MAIN_WORKER_POOL_NAME)
    public WorkerPool initializeWorkerThreadPool(
        ErrorReporter errorLog,
        DbConnectionPool dbConnPool
    )
    {
        return WorkerPoolInitializer.createDefaultWorkerThreadPool(
            errorLog,
            dbConnPool,
            "MainWorkerPool"
        );
    }
}
