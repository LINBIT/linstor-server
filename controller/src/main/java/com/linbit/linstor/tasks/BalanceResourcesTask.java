package com.linbit.linstor.tasks;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.BalanceResources;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
public class BalanceResourcesTask implements TaskScheduleService.Task
{
    public static final long DEFAULT_TASK_INTERVAL_SEC = 3600;
    public static final long DEFAULT_BALANCE_TIMEOUT_SEC = 6000;

    private final ErrorReporter log;
    private final SystemConfRepository systemConfRepository;
    private final BalanceResources balanceResources;

    @Inject
    public BalanceResourcesTask(
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        BalanceResources balanceResourcesRef
    )
    {
        log = errorReporterRef;
        systemConfRepository = systemConfRepositoryRef;
        balanceResources = balanceResourcesRef;
    }

    /**
     * Returns the next execution interval, depending on property or default
     * @return next execution interval in seconds
     */
    private long nextExecutionInterval()
    {
        long nextExecInterval = DEFAULT_TASK_INTERVAL_SEC;
        try
        {
            String taskIntervalProp = systemConfRepository.getCtrlConfForView()
                .getProp(ApiConsts.KEY_BALANCE_RESOURCES_INTERVAL);
            if (taskIntervalProp != null)
            {
                nextExecInterval = Long.parseLong(taskIntervalProp);
            }
        }
        catch (NumberFormatException nfe)
        {
            log.logError("%s property number format exception, fallback to default %d",
                ApiConsts.KEY_BALANCE_RESOURCES_INTERVAL,
                DEFAULT_TASK_INTERVAL_SEC);
            log.reportError(nfe);
        }
        return nextExecInterval;
    }

    @Override
    public long run(long scheduleAt)
    {
        log.logInfo("BalanceResourcesTask/START");
        long nextExecutionIntervalSecs = nextExecutionInterval();

        var result = balanceResources.balanceResources(DEFAULT_BALANCE_TIMEOUT_SEC);

        log.logInfo("BalanceResourcesTask/END: Adjusted: %d - Removed: %d", result.objA, result.objB);
        return getNextFutureReschedule(scheduleAt, nextExecutionIntervalSecs * 1000);
    }
}
