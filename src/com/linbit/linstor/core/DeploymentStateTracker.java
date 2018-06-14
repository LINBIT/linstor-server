package com.linbit.linstor.core;

import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiCallRc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class DeploymentStateTracker
{
    private final Map<ResourceName, ApiCallRc> deploymentStates;
    private final Map<ResourceName, List<SnapshotState>> snapshotStates;

    @Inject
    public DeploymentStateTracker()
    {
        deploymentStates = Collections.synchronizedMap(new HashMap<>());
        snapshotStates = Collections.synchronizedMap(new HashMap<>());
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

    public void setSnapshotStates(ResourceName resourceName, List<SnapshotState> state)
    {
        snapshotStates.put(resourceName, state);
    }

    public void removeSnapshotStates(ResourceName resourceName)
    {
        snapshotStates.remove(resourceName);
    }

    public List<SnapshotState> getSnapshotStates(ResourceName resourceName)
    {
        List<SnapshotState> resourceSnapshotStates = snapshotStates.get(resourceName);
        return resourceSnapshotStates == null ? Collections.emptyList() : resourceSnapshotStates;
    }
}
