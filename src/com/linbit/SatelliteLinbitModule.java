package com.linbit;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Singleton;

public class SatelliteLinbitModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    public WorkerPool initializeWorkerThreadPool(ErrorReporter errorLog)
    {
        return WorkerPoolInitializer.createDefaultWorkerThreadPool(
            errorLog,
            null,
            "MainWorkerPool"
        );
    }
}
