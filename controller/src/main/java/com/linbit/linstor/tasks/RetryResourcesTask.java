package com.linbit.linstor.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class RetryResourcesTask implements Task
{
    private static final long[] RETRY_DELAYS =
    {
        15_000,
        30_000,
        1 * 60_000,
        2 * 60_000,
        5 * 60_000,
        10 * 60_000,
        30 * 60_000,
        1 * 60 * 60_000,
        4 * 60 * 60_000,
        24 * 60 * 60_000,
    };
    private static final long TASK_TIMEOUT = RETRY_DELAYS[0];

    private final Object syncObj = new Object();
    private final HashMap<Resource, Pair<Integer, Long>> failedResources = new HashMap<>();

    private final AccessContext sysCtx;
    private final CtrlStltSerializer serializer;
    private final ErrorReporter errorReporter;

    @Inject
    public RetryResourcesTask(
        @SystemContext AccessContext sysCtxRef,
        CtrlStltSerializer serializerRef,
        ErrorReporter errorReporterRef
    )
    {
        sysCtx = sysCtxRef;
        serializer = serializerRef;
        errorReporter = errorReporterRef;
    }

    public void add(Resource rsc)
    {
        synchronized (syncObj)
        {
            if (!failedResources.containsKey(rsc))
            {
                failedResources.put(rsc, new Pair<>(0, System.currentTimeMillis()));
            }
        }
    }

    public void remove(Resource rsc)
    {
        synchronized (syncObj)
        {
            failedResources.remove(rsc);
        }
    }

    @Override
    public long run()
    {
        List<Resource> rscsToRetry;
        synchronized (syncObj)
        {
            rscsToRetry = getResourcesToRetry();
        }

        for (Resource rsc : rscsToRetry)
        {
            if (!rsc.isDeleted())
            {
                try
                {
                    if (!rsc.getAssignedNode().isDeleted())
                    {
                        Peer peer = rsc.getAssignedNode().getPeer(sysCtx);
                        // only update the one satellite, not every involved satellites
                        peer.sendMessage(
                            serializer
                                .onewayBuilder(InternalApiConsts.API_CHANGED_RSC)
                                .changedResource(
                                    rsc.getUuid(),
                                    rsc.getDefinition().getName().displayValue
                                )
                                .build()
                        );
                    }
                    else
                    {
                        failedResources.remove(rsc);
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    errorReporter.reportError(new ImplementationError(accDeniedExc));
                }
            }
        }
        return TASK_TIMEOUT;
    }

    private List<Resource> getResourcesToRetry()
    {
        List<Resource> ret = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Entry<Resource, Pair<Integer, Long>> entry : failedResources.entrySet())
        {
            Pair<Integer, Long> pair = entry.getValue();
            int retryIdx;
            int times = 1;

            if (pair.objA >= RETRY_DELAYS.length)
            {
                retryIdx = RETRY_DELAYS.length - 1;
                times = pair.objA - RETRY_DELAYS.length + 1;
            }
            else
            {
                retryIdx = pair.objA;
            }

            long retryAt = pair.objB + RETRY_DELAYS[retryIdx] * times;
            retryAt = (retryAt / TASK_TIMEOUT) * TASK_TIMEOUT;

            if (now >= retryAt)
            {
                ret.add(entry.getKey());
                pair.objA += 1;
                pair.objB = now;
            }
        }

        return ret;
    }
}
