package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Singleton;

public class ControllerLinbitModule extends AbstractModule
{
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
        return WorkerPoolInitializer.createDefaultWorkerThreadPool(
            errorLog,
            dbConnPool,
            "MainWorkerPool"
        );
    }
}
