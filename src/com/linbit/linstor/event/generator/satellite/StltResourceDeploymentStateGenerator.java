package com.linbit.linstor.event.generator.satellite;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.core.DeploymentStateTracker;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.generator.ResourceDeploymentStateGenerator;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StltResourceDeploymentStateGenerator implements ResourceDeploymentStateGenerator
{
    private final DeploymentStateTracker deploymentStateTracker;

    @Inject
    public StltResourceDeploymentStateGenerator(DeploymentStateTracker deploymentStateTrackerRef)
    {
        deploymentStateTracker = deploymentStateTrackerRef;
    }

    @Override
    public ApiCallRc generate(ObjectIdentifier objectIdentifier)
    {
        return deploymentStateTracker.getDeploymentState(objectIdentifier.getResourceName());
    }
}
