package com.linbit.linstor.tasks;

import com.linbit.linstor.core.apicallhandler.controller.CtrlQuerySizeInfoHelper;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class QsiClearCacheTask implements Task
{
    private static final long RERUN_IN_MS = 60 * 60 * 1_000L; // once per hour

    private final CtrlQuerySizeInfoHelper qsiHelper;

    @Inject
    public QsiClearCacheTask(
        CtrlQuerySizeInfoHelper qsiHelperRef
    )
    {
        qsiHelper = qsiHelperRef;
    }

    @Override
    public long run(long scheduledAtRef)
    {
        qsiHelper.clearCache();
        return getNextFutureReschedule(scheduledAtRef, RERUN_IN_MS);
    }
}
