package com.linbit.linstor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LinStorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    public Scheduler mainWorkerPoolScheduler()
    {
        return Schedulers.newParallel("MainWorkerPool");
    }
}
