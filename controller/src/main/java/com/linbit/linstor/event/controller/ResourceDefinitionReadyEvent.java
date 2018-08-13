package com.linbit.linstor.event.controller;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.event.LinstorEvent;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.ObjectSignal;
import com.linbit.linstor.event.common.ResourceDeploymentStateEvent;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.UsageState;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Signal;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class ResourceDefinitionReadyEvent implements LinstorEvent<ResourceDefinitionReadyEventData>
{
    private final ResourceStateEvent resourceStateEvent;
    private final ResourceDeploymentStateEvent resourceDeploymentStateEvent;

    @Inject
    public ResourceDefinitionReadyEvent(
        ResourceStateEvent resourceStateEventRef,
        ResourceDeploymentStateEvent resourceDeploymentStateEventRef
    )
    {
        resourceStateEvent = resourceStateEventRef;
        resourceDeploymentStateEvent = resourceDeploymentStateEventRef;
    }

    @Override
    public Flux<ObjectSignal<ResourceDefinitionReadyEventData>> watchForStreams(
        ObjectIdentifier ancestor
    )
    {
        return Flux.defer(() ->
            {
                ResourceDefinitionReadyEventMain resourceDefinitionReadyEventMain =
                    new ResourceDefinitionReadyEventMain();

                return Flux.merge(
                    resourceDefinitionReadyEventMain.convertResourceStateSignals(
                        resourceStateEvent.get().watchForStreams(ancestor)),
                    resourceDefinitionReadyEventMain.convertDeploymentStateSignals(
                        resourceDeploymentStateEvent.get().watchForStreams(ancestor))
                );
            }
        );
    }

    // Implement the stream merging using a stateful mapping class
    private static class ResourceDefinitionReadyEventMain
    {
        private final Lock lock = new ReentrantLock();
        private final Map<ResourceName, Map<NodeName, UsageState>> currentUsageStates = new HashMap<>();
        private final Map<ResourceName, Map<NodeName, ApiCallRc>> currentDeploymentStates = new HashMap<>();

        Flux<ObjectSignal<ResourceDefinitionReadyEventData>> convertResourceStateSignals(
            Flux<ObjectSignal<UsageState>> resourceStateFlux
        )
        {
            return convertSignals(resourceStateFlux, currentUsageStates);
        }

        Flux<ObjectSignal<ResourceDefinitionReadyEventData>> convertDeploymentStateSignals(
            Flux<ObjectSignal<ApiCallRc>> deploymentStateFlux
        )
        {
            return convertSignals(deploymentStateFlux, currentDeploymentStates);
        }

        private <T> Flux<ObjectSignal<ResourceDefinitionReadyEventData>> convertSignals(
            Flux<ObjectSignal<T>> flux, Map<ResourceName, Map<NodeName, T>> map
        )
        {
            return flux
                .map(objectSignal ->
                    {
                        ObjectSignal<ResourceDefinitionReadyEventData> outputSignal;
                        lock.lock();
                        try
                        {
                            outputSignal = convertSignal(map, objectSignal);
                        }
                        finally
                        {
                            lock.unlock();
                        }
                        return outputSignal;
                    }

                );
        }

        private <T> ObjectSignal<ResourceDefinitionReadyEventData> convertSignal(
            Map<ResourceName, Map<NodeName, T>> map,
            ObjectSignal<T> objectSignal
        )
        {
            ObjectIdentifier objectIdentifier = objectSignal.getObjectIdentifier();
            ResourceName resourceName = objectIdentifier.getResourceName();
            NodeName nodeName = objectIdentifier.getNodeName();

            Signal<T> signal = objectSignal.getSignal();
            if (signal.isOnNext())
            {
                map.computeIfAbsent(resourceName, ignored -> new HashMap<>()).put(nodeName, signal.get());
            }
            else
            {
                Map<NodeName, T> innerMap = map.get(resourceName);
                if (innerMap != null)
                {
                    innerMap.remove(nodeName);
                    if (innerMap.isEmpty())
                    {
                        map.remove(resourceName);
                    }
                }
            }

            Map<NodeName, UsageState> usageStates = currentUsageStates.get(resourceName);
            Map<NodeName, ApiCallRc> deploymentStates = currentDeploymentStates.get(resourceName);
            Signal<ResourceDefinitionReadyEventData> outSignal;
            if (usageStates == null && deploymentStates == null)
            {
                outSignal = Signal.complete();
            }
            else
            {
                outSignal = Signal.next(sumReadyCounts(
                    usageStates == null ? Collections.emptySet() : usageStates.values(),
                    deploymentStates == null ? Collections.emptySet() : deploymentStates.values()
                ));
            }
            return new ObjectSignal<>(ObjectIdentifier.resourceDefinition(resourceName), outSignal);
        }

        private ResourceDefinitionReadyEventData sumReadyCounts(
            Collection<UsageState> usageStates, Collection<ApiCallRc> deploymentStates
        )
        {
            int readyCount = 0;
            for (UsageState usageState : usageStates)
            {
                if (usageState.getResourceReady() != null && usageState.getResourceReady())
                {
                    readyCount++;
                }
            }

            int errorCount = 0;
            for (ApiCallRc apiCallRc : deploymentStates)
            {
                if (apiCallRc != null && ApiRcUtils.isError(apiCallRc))
                {
                    errorCount++;
                }
            }

            return new ResourceDefinitionReadyEventData(readyCount, errorCount);
        }
    }
}
