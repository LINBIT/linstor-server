package com.linbit.linstor;

import com.linbit.linstor.core.LinStor;
import com.linbit.utils.MathUtils;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class LinStorModule extends AbstractModule
{
    // Minimum number of worker threads
    private static final int MIN_THREAD_SIZE = 4;

    // Maximum number of worker threads
    private static final int MAX_THREAD_SIZE = 16;

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    public Scheduler mainWorkerPoolScheduler()
    {
        int thrCount = MathUtils.bounds(MIN_THREAD_SIZE, LinStor.CPU_COUNT, MAX_THREAD_SIZE);
        return Schedulers.newParallel("MainWorkerPool", thrCount);
    }
}
