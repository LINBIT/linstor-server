package com.linbit.linstor.satellitestate;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SatelliteState
{
    private final Map<ResourceName, SatelliteResourceState> resourceStates = new HashMap<>();

    public SatelliteState()
    {
    }

    public SatelliteState(SatelliteState other)
    {
        for (Map.Entry<ResourceName, SatelliteResourceState> resourceStateEntry : other.resourceStates.entrySet())
        {
            resourceStates.put(resourceStateEntry.getKey(), new SatelliteResourceState(resourceStateEntry.getValue()));
        }
    }

    public Map<ResourceName, SatelliteResourceState> getResourceStates()
    {
        return resourceStates;
    }

    public <T> @Nullable T getFromResource(ResourceName resourceName, Function<SatelliteResourceState, T> getter)
    {
        return resourceStates.containsKey(resourceName) ?
            getter.apply(resourceStates.get(resourceName)) :
            null;
    }

    public <T> void setOnResource(
        ResourceName resourceName,
        BiConsumer<SatelliteResourceState, T> setter,
        @Nullable T value
    )
    {
        setter.accept(
            resourceStates.computeIfAbsent(resourceName, ignored -> new SatelliteResourceState()),
            value
        );
    }

    public <T> void unsetOnResource(
        ResourceName resourceName,
        BiConsumer<SatelliteResourceState, T> setter
    )
    {
        SatelliteResourceState resourceState = resourceStates.get(resourceName);
        if (resourceState != null)
        {
            setter.accept(resourceState, null);

            if (resourceState.isEmpty())
            {
                resourceStates.remove(resourceName);
            }
        }
    }

    public <T> @Nullable T getFromVolume(
        ResourceName resourceName,
        VolumeNumber volumeNumber,
        Function<SatelliteVolumeState, T> getter
    )
    {
        T value;

        SatelliteResourceState resourceState = resourceStates.get(resourceName);
        if (resourceState == null)
        {
            value = null;
        }
        else
        {
            value = resourceState.getFromVolume(volumeNumber, getter);
        }

        return value;
    }

    public <T> void setOnVolume(
        ResourceName resourceName,
        VolumeNumber volumeNumber,
        BiConsumer<SatelliteVolumeState, T> setter,
        T value
    )
    {
        resourceStates.computeIfAbsent(resourceName, ignored -> new SatelliteResourceState())
            .setOnVolume(volumeNumber, setter, value);
    }

    public <T> void unsetOnVolume(
        ResourceName resourceName,
        VolumeNumber volumeNumber,
        BiConsumer<SatelliteVolumeState, T> setter
    )
    {
        SatelliteResourceState resourceState = resourceStates.get(resourceName);
        if (resourceState != null)
        {
            resourceState.unsetOnVolume(volumeNumber, setter);

            if (resourceState.isEmpty())
            {
                resourceStates.remove(resourceName);
            }
        }
    }

    public void setOnConnection(
        ResourceName resourceName,
        NodeName node1,
        NodeName node2,
        String value)
    {
        resourceStates.computeIfAbsent(resourceName, ignored -> new SatelliteResourceState())
            .setOnConnection(node1, node2, value);
    }

    public void unsetOnConnection(
        ResourceName resourceName,
        NodeName node1,
        NodeName node2)
    {
        SatelliteResourceState resourceState = resourceStates.get(resourceName);
        if (resourceState != null)
        {
            resourceState.unsetConnection(node1, node2);

            if (resourceState.isEmpty())
            {
                resourceStates.remove(resourceName);
            }
        }
    }
}
