package com.linbit.linstor.tasks;

import com.linbit.linstor.tasks.TaskScheduleService.Task;

public class GarbageCollectorTask implements Task
{
    private static final long GC_SLEEP = 600_000; // 10 min

    @Override
    public long run()
    {
        System.gc();
        return GC_SLEEP;
    }

}
