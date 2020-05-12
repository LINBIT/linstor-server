package com.linbit.linstor.satellitestate;

import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SatelliteResourceState
{
    private Boolean inUse;

    private final Map<VolumeNumber, SatelliteVolumeState> volumeStates = new HashMap<>();
    private final Map<NodeName, Map<NodeName, String>> connectionStates = new HashMap<>();

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
        for (Map.Entry<NodeName, Map<NodeName, String>> connEntry : other.connectionStates.entrySet())
        {
            connectionStates.put(connEntry.getKey(), new HashMap<>(connEntry.getValue()));
        }
    }

    public @Nullable Boolean isInUse()
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

    public void setOnConnection(NodeName sourceNode, NodeName targetNode, String value)
    {
        Map<NodeName, String> connections = connectionStates.computeIfAbsent(sourceNode, ignored -> new HashMap<>());
        connections.put(targetNode, value);
    }

    public void unsetConnection(NodeName sourceNode, NodeName targetNode)
    {
        Map<NodeName, String> connections = connectionStates.get(sourceNode);
        connections.remove(targetNode);
        if (connections.isEmpty())
        {
            connectionStates.remove(sourceNode);
        }
    }

    public Map<NodeName, Map<NodeName, String>> getConnectionStates()
    {
        return connectionStates;
    }

    public boolean isEmpty()
    {
        return volumeStates.isEmpty() && connectionStates.isEmpty();
    }
}
