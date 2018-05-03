package com.linbit.linstor.event.generator.satellite;

import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.drbdstate.DrbdConnection;
import com.linbit.linstor.drbdstate.DrbdEventService;
import com.linbit.linstor.drbdstate.DrbdResource;
import com.linbit.linstor.drbdstate.DrbdVolume;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.generator.ResourceStateGenerator;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Singleton
public class StltResourceStateGenerator implements ResourceStateGenerator
{
    private static final List<DrbdVolume.ReplState> USABLE_REPLICATING_STATES = Arrays.asList(
        DrbdVolume.ReplState.ESTABLISHED,
        DrbdVolume.ReplState.SYNC_TARGET
    );

    private final DrbdEventService drbdEventService;

    @Inject
    public StltResourceStateGenerator(DrbdEventService drbdEventServiceRef)
    {
        drbdEventService = drbdEventServiceRef;
    }

    @Override
    public Boolean generate(ObjectIdentifier objectIdentifier)
        throws Exception
    {
        Boolean resourceReady = null;

        if (drbdEventService.isDrbdStateAvailable())
        {
            DrbdResource drbdResource =
                drbdEventService.getDrbdResource(objectIdentifier.getResourceName().displayValue);
            if (drbdResource != null)
            {
                Map<VolumeNumber, DrbdVolume> volumesMap = drbdResource.getVolumesMap();
                resourceReady = !volumesMap.isEmpty() &&
                    volumesMap.values().stream().allMatch(this::volumeReady);
            }
        }

        return resourceReady;
    }

    private boolean volumeReady(DrbdVolume volume)
    {
        boolean ready;

        if (volume.getDiskState() == DrbdVolume.DiskState.UP_TO_DATE)
        {
            ready = true;
        }
        else
        {
            ready = volume.getResource().getConnectionsMap().values().stream()
                .anyMatch(drbdConnection -> peerVolumeUsable(drbdConnection, volume.getVolNr()));
        }

        return ready;
    }

    private boolean peerVolumeUsable(DrbdConnection connection, VolumeNumber volumeNumber)
    {
        DrbdVolume peerVolume = connection.getVolume(volumeNumber);
        return peerVolume != null && USABLE_REPLICATING_STATES.contains(peerVolume.getReplState());
    }
}
