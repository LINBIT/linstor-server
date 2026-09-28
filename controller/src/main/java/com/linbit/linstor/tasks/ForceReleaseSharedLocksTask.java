package com.linbit.linstor.tasks;

import com.linbit.linstor.core.SharedStorPoolManager;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

public class ForceReleaseSharedLocksTask implements Task
{
    private static final int DEFAULT_DELAY = 10_000;

    private boolean needsDelay = true;
    private int delay = DEFAULT_DELAY;

    private final NodeName nodeName;
    private final SharedStorPoolManager sharedSPMgr;
    private final NodeInternalCallHandler nodeInternalCallHandler;

    public ForceReleaseSharedLocksTask(
        Node nodeRef,
        SharedStorPoolManager sharedSPMgrRef,
        NodeInternalCallHandler nodeInternalCallHandlerRef
    )
    {
        nodeName = nodeRef.getKey();
        sharedSPMgr = sharedSPMgrRef;
        nodeInternalCallHandler = nodeInternalCallHandlerRef;
    }

    @Override
    public long run(long scheduleAt)
    {
        long ret;
        if (needsDelay)
        {
            needsDelay = false;
            sharedSPMgr.forgetRequests(nodeName);
            ret = getNextFutureReschedule(scheduleAt, delay);
        }
        else
        {
            nodeInternalCallHandler.releaseLocks(nodeName);
            ret = END_TASK;
        }
        return ret;
    }
}
