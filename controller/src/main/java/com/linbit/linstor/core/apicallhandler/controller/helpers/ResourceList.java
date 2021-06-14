package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.satellitestate.SatelliteState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ResourceList
{
    private final ArrayList<ResourceApi> rscs = new ArrayList<>();
    private final Map<NodeName, SatelliteState> satelliteStates = new HashMap<>();

    public void addResource(ResourceApi rscApi)
    {
        rscs.add(rscApi);
    }

    public void putSatelliteState(NodeName nodeName, SatelliteState satelliteState)
    {
        satelliteStates.put(nodeName, satelliteState);
    }

    public ArrayList<ResourceApi> getResources()
    {
        return rscs;
    }

    public boolean isEmpty()
    {
        return rscs.isEmpty();
    }

    public Map<NodeName, SatelliteState> getSatelliteStates()
    {
        return satelliteStates;
    }
}
