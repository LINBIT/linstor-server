package com.linbit.linstor.satellitestate;

import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.utils.PairNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SatelliteVolumeState
{
    private @Nullable String diskState;
    /**
     * Stores the replication states for each node connection
     */
    private Map<NodeName, ReplState> replicationStateMap = new HashMap<>();
    /**
     * Stores the done percentage for each node connection
     */
    private Map<NodeName, Float> donePercentageMap = new HashMap<>();

    public SatelliteVolumeState()
    {
    }

    public SatelliteVolumeState(SatelliteVolumeState other)
    {
        diskState = other.diskState;
        replicationStateMap = other.replicationStateMap;
        donePercentageMap = other.donePercentageMap;
    }

    public @Nullable String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public Map<NodeName, ReplState> getReplicationStateMap()
    {
        return replicationStateMap;
    }

    public void setReplicationState(@Nullable PairNonNull<String, ReplState> replStatePair)
    {
        if (replStatePair != null)
        {
            NodeName nodeName = LinstorParsingUtils.asNodeName(replStatePair.objA);
            if (replStatePair.objB == ReplState.OFF)
            {
                replicationStateMap.remove(nodeName);
            }
            else
            {
                replicationStateMap.put(nodeName, replStatePair.objB);
            }
        }
        else
        {
            replicationStateMap.clear();
        }
    }

    public Map<NodeName, Float> getDonePercentageMap()
    {
        return donePercentageMap;
    }

    public void setDonePercentage(@Nullable PairNonNull<String, Optional<Float>> donePercentagePair)
    {
        if (donePercentagePair != null)
        {
            NodeName nodeName = LinstorParsingUtils.asNodeName(donePercentagePair.objA);
            if (donePercentagePair.objB.isEmpty())
            {
                donePercentageMap.remove(nodeName);
            }
            else
            {
                donePercentageMap.put(nodeName, donePercentagePair.objB.get());
            }
        }
        else
        {
            donePercentageMap.clear();
        }
    }

    public boolean isEmpty()
    {
        return diskState == null && replicationStateMap.isEmpty() && donePercentageMap.isEmpty();
    }
}
