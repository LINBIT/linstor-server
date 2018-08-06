package com.linbit.linstor;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LinStorModule extends AbstractModule
{
    public static final String EVENT_WRITER_WORKER_POOL_NAME = "EventWriterWorkerPool";

    public static final String TRANS_MGR_GENERATOR = "transMgrGenerator";

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
