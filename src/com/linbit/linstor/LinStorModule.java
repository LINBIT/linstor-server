package com.linbit.linstor;

import com.google.inject.AbstractModule;

public class LinStorModule extends AbstractModule
{
    // Name for main worker pool, e.g. API call handling
    public static final String MAIN_WORKER_POOL_NAME = "MainWorkerPool";

    public static final String EVENT_WRITER_WORKER_POOL_NAME = "EventWriterWorkerPool";

    public static final String TRANS_MGR_GENERATOR = "transMgrGenerator";

    @Override
    protected void configure()
    {
    }
}
