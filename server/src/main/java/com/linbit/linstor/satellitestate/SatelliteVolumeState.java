package com.linbit.linstor.satellitestate;

import com.linbit.linstor.layer.drbd.drbdstate.ReplState;

public class SatelliteVolumeState
{
    private String diskState;
    private ReplState replicationState;

    public SatelliteVolumeState()
    {
    }

    public SatelliteVolumeState(SatelliteVolumeState other)
    {
        diskState = other.diskState;
        replicationState = other.replicationState;
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

    public boolean isEmpty()
    {
        return diskState == null && replicationState == null;
    }
}
