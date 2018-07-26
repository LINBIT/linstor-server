package com.linbit.linstor.satellitestate;

public class SatelliteVolumeState
{
    private String diskState;

    public SatelliteVolumeState()
    {
    }

    public SatelliteVolumeState(SatelliteVolumeState other)
    {
        diskState = other.diskState;
    }

    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    public boolean isEmpty()
    {
        return diskState == null;
    }
}
