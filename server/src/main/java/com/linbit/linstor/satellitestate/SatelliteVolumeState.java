package com.linbit.linstor.satellitestate;

import com.linbit.linstor.layer.drbd.drbdstate.ReplState;
import com.linbit.utils.Pair;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SatelliteVolumeState
{
    private String diskState;
    /**
     * Stores the replication states for each peer connection, key is the peer name, NOT the node name
     */
    private Map<String, ReplState> replicationStateMap = new HashMap<>();
    /**
     * Stores the done percentage for each peer connection, key is the peer name, NOT the node name
     */
    private Map<String, Float> donePercentageMap = new HashMap<>();

    public SatelliteVolumeState()
    {
    }

    public SatelliteVolumeState(SatelliteVolumeState other)
    {
        diskState = other.diskState;
        replicationStateMap = other.replicationStateMap;
        donePercentageMap = other.donePercentageMap;
    }

    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public Map<String, ReplState> getReplicationStateMap()
    {
        return replicationStateMap;
    }

    public void setReplicationState(@Nullable Pair<String, ReplState> replStatePair)
    {
        if (replStatePair != null)
        {
            if (replStatePair.objB == null || replStatePair.objB == ReplState.OFF)
            {
                replicationStateMap.remove(replStatePair.objA);
            }
            else
            {
                replicationStateMap.put(replStatePair.objA, replStatePair.objB);
            }
        }
        else
        {
            replicationStateMap.clear();
        }
    }

    public Map<String, Float> getDonePercentageMap()
    {
        return donePercentageMap;
    }

    public void setDonePercentage(@Nullable Pair<String, Optional<Float>> donePercentagePair)
    {
        if (donePercentagePair != null)
        {
            if (donePercentagePair.objB.isEmpty())
            {
                donePercentageMap.remove(donePercentagePair.objA);
            }
            else
            {
                donePercentageMap.put(donePercentagePair.objA, donePercentagePair.objB.get());
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
