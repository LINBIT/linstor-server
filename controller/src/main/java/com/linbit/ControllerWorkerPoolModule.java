package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Named;
import javax.inject.Singleton;

public class ControllerWorkerPoolModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    @Named(LinStorModule.EVENT_WRITER_WORKER_POOL_NAME)
    public WorkQueue initializeEventWriterWorkerThreadPool(
        ErrorReporter errorLog,
        DbConnectionPool dbConnPool
    )
    {
        return WorkerPoolInitializer.createDefaultWorkerThreadPool(
            errorLog,
            dbConnPool,
            "EventWriterWorkerPool"
        );
    }
}
