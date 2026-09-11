package com.linbit.linstor.tasks;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.netcom.PeerNotConnectedException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;

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
    private static final String RSC_RETRY_API_NAME = "RetryResource";

    private final Object syncObj = new Object();
    private final HashMap<Resource, RetryConfig> failedResources = new HashMap<>();

    private final CtrlStltSerializer serializer;
    private final ErrorReporter errorReporter;

    @Inject
    public RetryResourcesTask(
        CtrlStltSerializer serializerRef,
        ErrorReporter errorReporterRef
    )
    {
        serializer = serializerRef;
        errorReporter = errorReporterRef;
    }

    public boolean add(Resource rsc, @Nullable Publisher<ApiCallRc> nextStepRef)
    {
        boolean added = false;
        synchronized (syncObj)
        {
            RetryConfig config = failedResources.get(rsc);
            if (config == null)
            {
                added = true;
                failedResources.put(rsc, new RetryConfig(rsc, nextStepRef));
            }
            else
            if (config.fluxAfterSuccess == null)
            {
                config.fluxAfterSuccess = nextStepRef;
            }
        }
        if (added)
        {
            errorReporter.logWarning(
                "RetryTask: Failed resource '%s' of node '%s' added for retry.",
                rsc.getKey().getResourceName().displayValue,
                rsc.getKey().getNodeName().displayValue
            );
        }
        return added;
    }

    public void remove(Resource rsc)
    {
        Object removed = null;
        synchronized (syncObj)
        {
            removed = failedResources.remove(rsc);
        }

        if (removed != null)
        {
            errorReporter.logInfo(
                "RetryTask: Failed resource '%s' of node '%s' removed from retry.",
                rsc.getKey().getResourceName().displayValue,
                rsc.getKey().getNodeName().displayValue
            );
        }
    }

    @Override
    public long run(long scheduleAt)
    {
        List<RetryConfig> rscsToRetry;
        synchronized (syncObj)
        {
            rscsToRetry = getResourcesToRetry();
        }

        for (RetryConfig retryConfig : rscsToRetry)
        {
            Resource rsc = retryConfig.rsc;
            if (!rsc.isDeleted())
            {
                Node node = rsc.getNode();
                if (!node.isDeleted())
                {
                    Peer peer = node.getPeer();
                    NodeName nodeName = node.getName();

                    errorReporter.logDebug(
                        "RetryTask: Contact satellite '%s' to retry resource '%s'.",
                        nodeName.displayValue,
                        rsc.getResourceDefinition().getName().displayValue
                    );

                    // only update the one satellite, not every involved satellites
                    Publisher<ApiCallRc> nextStep = retryConfig.fluxAfterSuccess;
                    if (nextStep == null)
                    {
                        nextStep = Flux.empty();
                    }

                    peer.apiCall(
                        InternalApiConsts.API_CHANGED_RSC,
                        serializer
                            .headerlessBuilder()
                            .changedResource(
                                rsc.getUuid(),
                                rsc.getResourceDefinition().getName().displayValue
                            )
                            .build()
                    ).map(
                        inputStream -> CtrlSatelliteUpdateCaller.deserializeApiCallRc(
                            nodeName,
                            inputStream
                        )
                    )
                        .concatWith(nextStep)
                        .onErrorResume(ApiRcException.class, ignore -> Flux.empty())
                        .onErrorResume(PeerNotConnectedException.class, ignore -> Flux.empty())
                        .contextWrite(
                            Context.of(
                                ApiModule.API_CALL_NAME,
                                RSC_RETRY_API_NAME,
                                Peer.class, peer
                            )
                        )
                        .subscribe();
                }
                else
                {
                    remove(rsc);
                }
            }
            else
            {
                remove(rsc);
            }
        }
        return getNextFutureReschedule(scheduleAt, TASK_TIMEOUT);
    }

    private List<RetryConfig> getResourcesToRetry()
    {
        List<RetryConfig> ret = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (RetryConfig config : failedResources.values())
        {
            int retryIdx;
            int times = 1;

            if (config.retryTimes >= RETRY_DELAYS.length)
            {
                retryIdx = RETRY_DELAYS.length - 1;
                times = config.retryTimes - RETRY_DELAYS.length + 1;
            }
            else
            {
                retryIdx = config.retryTimes;
            }

            long retryAt = config.lastFailTimestamp + RETRY_DELAYS[retryIdx] * times;
            // round up instead of down. retrying too early usually does not make too much sense.
            retryAt = ((retryAt / TASK_TIMEOUT) + 1) * TASK_TIMEOUT;

            if (now >= retryAt)
            {
                ret.add(config);
                config.retryTimes += 1;
                config.lastFailTimestamp = now;
            }
        }

        return ret;
    }

    private static class RetryConfig
    {
        private final Resource rsc;

        private @Nullable Publisher<ApiCallRc> fluxAfterSuccess;
        private long lastFailTimestamp;
        private int retryTimes;

        private RetryConfig(Resource rscRef, @Nullable Publisher<ApiCallRc> fluxAfterSuccessRef)
        {
            rsc = rscRef;
            fluxAfterSuccess = fluxAfterSuccessRef;

            retryTimes = 0;
            lastFailTimestamp = System.currentTimeMillis();
        }
    }
}
