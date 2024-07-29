package com.linbit.linstor.satellitestate;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.drbd.drbdstate.DiskState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SatelliteResourceState
{
    private @Nullable Boolean inUse;
    private boolean isReady;

    private final Map<VolumeNumber, SatelliteVolumeState> volumeStates = new HashMap<>();
    private final Map<NodeName, Map<NodeName, String>> connectionStates = new HashMap<>();
    private final Map<VolumeNumber, Map<Integer, Boolean>> peersConnected = new HashMap<>();

    public SatelliteResourceState()
    {
    }

    public SatelliteResourceState(SatelliteResourceState other)
    {
        inUse = other.inUse;
        isReady = other.isReady;
        for (Map.Entry<VolumeNumber, SatelliteVolumeState> volumeStateEntry : other.volumeStates.entrySet())
        {
            volumeStates.put(volumeStateEntry.getKey(), new SatelliteVolumeState(volumeStateEntry.getValue()));
        }
        for (Map.Entry<NodeName, Map<NodeName, String>> connEntry : other.connectionStates.entrySet())
        {
            connectionStates.put(connEntry.getKey(), new HashMap<>(connEntry.getValue()));
        }
        synchronized (other.peersConnected)
        {
            copyPeersConnected(other.peersConnected);
        }
    }

    private void copyPeersConnected(Map<VolumeNumber, Map<Integer, Boolean>> peersConnectedRef)
    {
        for (Map.Entry<VolumeNumber, Map<Integer, Boolean>> peersConEntry : peersConnectedRef.entrySet())
        {
            peersConnected.put(peersConEntry.getKey(), new HashMap<>(peersConEntry.getValue()));
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

    /**
     * Check if resource is ready and connected to the given node ids
     * @param expectedDiskfulOnlineNodeIds Should only contain diskfull node ids
     * @return true if resource connected and ready
     */
    public boolean isReady(Collection<Integer> expectedDiskfulOnlineNodeIds)
    {
        return isReady && ResourceState.allExpectedPeersOnline(expectedDiskfulOnlineNodeIds, peersConnected);
    }

    public void setIsReady(Boolean value)
    {
        isReady = value;
    }

    public void setPeersConnected(
        Map<VolumeNumber, Map<Integer/* nodeId */, Boolean/* connected */>> peersConnectedRef
    )
    {
        synchronized (peersConnected)
        {
            peersConnected.clear();
            copyPeersConnected(peersConnectedRef);
        }
    }

    public Map<VolumeNumber, Map<Integer/* nodeId */, Boolean/* connected */>> getPeersConnected()
    {
        synchronized (peersConnected)
        {
            return peersConnected;
        }
    }

    public Map<VolumeNumber, SatelliteVolumeState> getVolumeStates()
    {
        return volumeStates;
    }

    public boolean allVolumesUpToDate()
    {
        return volumeStates.values().stream()
            .allMatch(svs -> svs.getDiskState().equalsIgnoreCase(DiskState.UP_TO_DATE.toString()));
    }

    public <T> @Nullable T getFromVolume(VolumeNumber volumeNumber, Function<SatelliteVolumeState, T> getter)
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
