package com.linbit.linstor.satellitestate;

import com.linbit.linstor.layer.drbd.drbdstate.ReplState;

public class SatelliteVolumeState
{
    private String diskState;
    private ReplState replicationState;
    private Float donePercentage;

    public SatelliteVolumeState()
    {
    }

    public SatelliteVolumeState(SatelliteVolumeState other)
    {
        diskState = other.diskState;
        replicationState = other.replicationState;
        donePercentage = other.donePercentage;
    }

    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public ReplState getReplicationState()
    {
        return replicationState;
    }

    public void setReplicationState(ReplState replicationStateRef)
    {
        this.replicationState = replicationStateRef;
    }

    public Float getDonePercentage()
    {
        return donePercentage;
    }

    public void setDonePercentage(Float donePercentageRef)
    {
        donePercentage = donePercentageRef;
    }

    public boolean isEmpty()
    {
        return diskState == null && replicationState == null && donePercentage == null;
    }
}
