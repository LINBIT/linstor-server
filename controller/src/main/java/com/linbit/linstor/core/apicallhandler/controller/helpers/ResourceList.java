package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceData;
import com.linbit.linstor.satellitestate.SatelliteState;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ResourceList
{
    private ArrayList<ResourceData.RscApi> rscs = new ArrayList<>();
    private Map<NodeName, SatelliteState> satelliteStates = new HashMap<>();

    public void addResource(ResourceData.RscApi rscApi)
    {
        rscs.add(rscApi);
    }

    public void putSatelliteState(NodeName nodeName, SatelliteState satelliteState)
    {
        satelliteStates.put(nodeName, satelliteState);
    }

    public ArrayList<ResourceData.RscApi> getResources()
    {
        return rscs;
    }

    public Map<NodeName, SatelliteState> getSatelliteStates()
    {
        return satelliteStates;
    }
}
