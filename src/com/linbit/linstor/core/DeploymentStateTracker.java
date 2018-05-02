package com.linbit.linstor.core;

import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCallRc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Singleton
public class DeploymentStateTracker
{
    private final Map<ResourceName, ApiCallRc> deploymentStates;

    @Inject
    public DeploymentStateTracker()
    {
        deploymentStates = Collections.synchronizedMap(new HashMap<>());
    }

    public void setDeploymentState(ResourceName resourceName, ApiCallRc state)
    {
        deploymentStates.put(resourceName, state);
    }

    public void removeDeploymentState(ResourceName resourceName)
    {
        deploymentStates.remove(resourceName);
    }

    public ApiCallRc getDeploymentState(ResourceName resourceName)
    {
        return deploymentStates.get(resourceName);
    }
}
