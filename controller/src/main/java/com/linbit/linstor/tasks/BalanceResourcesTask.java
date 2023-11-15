package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.BalanceResources;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class BalanceResourcesTask implements TaskScheduleService.Task
{
    public static final long DEFAULT_TASK_INTERVAL_SEC = 3600;
    public static final long DEFAULT_BALANCE_TIMEOUT_SEC = 6000;

    private final AccessContext sysCtx;
    private final ErrorReporter log;
    private final SystemConfRepository systemConfRepository;
    private final BalanceResources balanceResources;

    @Inject
    public BalanceResourcesTask(
        @SystemContext AccessContext sysCtxRef,
        ErrorReporter errorReporterRef,
        SystemConfRepository systemConfRepositoryRef,
        BalanceResources balanceResourcesRef
    )
    {
        sysCtx = sysCtxRef;
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
            String taskIntervalProp = systemConfRepository.getCtrlConfForView(sysCtx)
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
        catch (AccessDeniedException exc)
        {
            log.reportError(new ImplementationError(exc));
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
