package com.linbit.linstor.satellitestate;

import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SatelliteResourceState
{
    private Boolean inUse;

    private final Map<VolumeNumber, SatelliteVolumeState> volumeStates = new HashMap<>();

    public SatelliteResourceState()
    {
    }

    public SatelliteResourceState(SatelliteResourceState other)
    {
        inUse = other.inUse;
        for (Map.Entry<VolumeNumber, SatelliteVolumeState> volumeStateEntry : other.volumeStates.entrySet())
        {
            volumeStates.put(volumeStateEntry.getKey(), new SatelliteVolumeState(volumeStateEntry.getValue()));
        }
    }

    public Boolean isInUse()
    {
        return inUse;
    }

    public void setInUse(Boolean value)
    {
        inUse = value;
    }

    public Map<VolumeNumber, SatelliteVolumeState> getVolumeStates()
    {
        return volumeStates;
    }

    public <T> T getFromVolume(VolumeNumber volumeNumber, Function<SatelliteVolumeState, T> getter)
    {
        return volumeStates.containsKey(volumeNumber) ?
            getter.apply(volumeStates.get(volumeNumber)) :
            null;
    }

    public <T> void setOnVolume(VolumeNumber volumeNumber, BiConsumer<SatelliteVolumeState, T> setter, T value)
    {
        volumeStates.computeIfAbsent(volumeNumber, ignored -> new SatelliteVolumeState());
        setter.accept(volumeStates.get(volumeNumber), value);
    }

    public <T> void unsetOnVolume(VolumeNumber volumeNumber, BiConsumer<SatelliteVolumeState, T> setter)
    {
        SatelliteVolumeState volumeState = volumeStates.get(volumeNumber);
        if (volumeState != null)
        {
            setter.accept(volumeState, null);

            if (volumeState.isEmpty())
            {
                volumeStates.remove(volumeNumber);
            }
        }
    }

    public boolean isEmpty()
    {
        return volumeStates.isEmpty();
    }
}
