package com.linbit.linstor.tasks;

import com.linbit.linstor.core.ebs.EbsStatusManagerService;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

public class EbsStatusPollTask implements Task
{
    private final EbsStatusManagerService ebsStatusMgr;
    private final long runEveryMs;

    public EbsStatusPollTask(EbsStatusManagerService ebsStatusMgrRef, long runEveryMsRef)
    {
        ebsStatusMgr = ebsStatusMgrRef;
        runEveryMs = runEveryMsRef;
    }

    @Override
    public long run(long scheduledAtRef)
    {
        ebsStatusMgr.pollAsync();
        return scheduledAtRef + runEveryMs;
    }
}
