package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ConnectionStateEvent;
import com.linbit.linstor.event.common.DonePercentageEvent;
import com.linbit.linstor.event.common.ReplicationStateEvent;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Publishes DRBD events as LinStor events.
 */
@Singleton
public class DrbdEventPublisher implements SystemService, ResourceObserver
{
    private static final ServiceName SERVICE_NAME;
    private static final String INSTANCE_PREFIX = "DrbdEventPublisher-";
    private static final String SERVICE_INFO = "DrbdEventPublisher";
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);

    private final DrbdEventService drbdEventService;
    private final ResourceStateEvent resourceStateEvent;
    private final VolumeDiskStateEvent volumeDiskStateEvent;
    private final ReplicationStateEvent replicationStateEvent;
    private final DonePercentageEvent donePercentageEvent;
    private final ConnectionStateEvent connectionStateEvent;

    private ServiceName instanceName;
    private boolean started = false;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("DrbdEventPublisher");
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public DrbdEventPublisher(
        DrbdEventService drbdEventServiceRef,
        ResourceStateEvent resourceStateEventRef,
        VolumeDiskStateEvent volumeDiskStateEventRef,
        ReplicationStateEvent replicationStateEventRef,
        DonePercentageEvent donePercentageEventRef,
        ConnectionStateEvent connectionStateEventRef
    )
    {
        drbdEventService = drbdEventServiceRef;
        resourceStateEvent = resourceStateEventRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;
        replicationStateEvent = replicationStateEventRef;
        donePercentageEvent = donePercentageEventRef;
        connectionStateEvent = connectionStateEventRef;

        try
        {
            instanceName = new ServiceName(INSTANCE_PREFIX + INSTANCE_COUNT.incrementAndGet());
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return instanceName;
    }

    @Override
    public boolean isStarted()
    {
        return started;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceNameRef)
    {
        instanceName = instanceNameRef;
    }

    @Override
    public void start()
    {
        drbdEventService.addObserver(this, DrbdStateTracker.OBS_ALL);
        started = true;
    }

    @Override
    public void shutdown()
    {
        drbdEventService.removeObserver(this);
        started = false;
    }

    @Override
    public void awaitShutdown(long timeout)
    {
        // Nothing to do
    }

    @Override
    public void resourceCreated(DrbdResource resource)
    {
        if (resource.isKnownByLinstor())
        {
            triggerResourceStateEvent(resource);
        }
    }

    @Override
    public void promotionScoreChanged(DrbdResource resource, Integer prevPromitionScore, Integer current)
    {
        if (resource.isKnownByLinstor())
        {
            triggerResourceStateEvent(resource);
        }
    }

    @Override
    public void mayPromoteChanged(DrbdResource resource, @Nullable Boolean prevMayPromote, @Nullable Boolean current)
    {
        if (resource.isKnownByLinstor())
        {
            triggerResourceStateEvent(resource);
        }
    }

    @Override
    public void resourceDestroyed(DrbdResource resource)
    {
        if (resource.isKnownByLinstor())
        {
            resourceStateEvent.get().closeStream(ObjectIdentifier.resourceDefinition(resource.getResName()));
        }
    }

    @Override
    public void volumeCreated(
        DrbdResource resource, DrbdConnection connection, DrbdVolume volume
    )
    {
        if (connection == null && resource.isKnownByLinstor())
        {
            triggerVolumeDiskStateEvent(resource, volume);
        }
    }

    @Override
    public void volumeDestroyed(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume
    )
    {
        if (connection == null && resource.isKnownByLinstor())
        {
            replicationStateEvent.get().closeStream(
                ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()));
            volumeDiskStateEvent.get().closeStream(
                ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()));
        }
    }

    @Override
    public void diskStateChanged(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume,
        DiskState previous,
        DiskState current
    )
    {
        if (resource.isKnownByLinstor())
        {
            if (connection == null)
            {
                triggerVolumeDiskStateEvent(resource, volume);
            }
            triggerResourceStateEvent(resource);
        }
    }

    @Override
    public void replicationStateChanged(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume,
        ReplState previous,
        ReplState current
    )
    {
        if (resource.isKnownByLinstor())
        {
            triggerReplicationStateEvent(resource, volume);
            triggerResourceStateEvent(resource);
        }
    }

    @Override
    public void donePercentageChanged(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume,
        Float previous,
        Float current
    )
    {
        if (resource.isKnownByLinstor())
        {
            triggerDonePercentageEvent(resource, volume);
        }
    }

    @Override
    public void roleChanged(DrbdResource resource, DrbdResource.Role previous, DrbdResource.Role current)
    {
        if (resource.isKnownByLinstor())
        {
            triggerResourceStateEvent(resource);
        }
    }

    private void triggerResourceStateEvent(DrbdResource resource)
    {
        resourceStateEvent.get().triggerEvent(
            ObjectIdentifier.resourceDefinition(resource.getResName()),
            determineResourceState(resource)
        );
    }

    private void triggerReplicationStateEvent(DrbdResource resource, DrbdVolume volume)
    {
        replicationStateEvent.get().triggerEvent(
            ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()),
            volume.replicationStateInfo()
        );
    }

    private void triggerDonePercentageEvent(DrbdResource resource, DrbdVolume volume)
    {
        donePercentageEvent.get().triggerEvent(
            ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()),
            Optional.ofNullable(volume.donePercentage)
        );
    }

    private void triggerVolumeDiskStateEvent(DrbdResource resource, DrbdVolume volume)
    {
        volumeDiskStateEvent.get().triggerEvent(
            ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()),
            volume.diskStateInfo()
        );
    }

    private ResourceState determineResourceState(DrbdResource drbdResource)
    {
        Map<VolumeNumber, DrbdVolume> volumesMap = drbdResource.getVolumesMap();

        return new ResourceState(
            !volumesMap.isEmpty() && volumesMap.values().stream().allMatch(this::allVolumesAccessToUpToDateData),
            connectedToPeers(volumesMap),
            drbdResource.getRole() == DrbdResource.Role.PRIMARY,
            volumesMap.values().stream().map(DrbdVolume::getDiskState)
                .allMatch(DiskState.UP_TO_DATE::equals),
            drbdResource.getPromotionScore(),
            drbdResource.mayPromote()
        );
    }

    private boolean allVolumesAccessToUpToDateData(DrbdVolume volume)
    {
        boolean accessUpToDateData;

        if (volume.getDiskState() == DiskState.UP_TO_DATE)
        {
            accessUpToDateData = true;
        }
        else
        {
            accessUpToDateData = volume.getResource().getConnectionsMap().values().stream()
                .anyMatch(drbdConnection -> peerVolumeUpToDate(drbdConnection, volume.getVolNr()));
        }

        return accessUpToDateData;
    }

    private Map<VolumeNumber, Map<Integer /* peer-node-id */, Boolean /* peer connected */>> connectedToPeers(
        Map<VolumeNumber, DrbdVolume> volumesMapRef
    )
    {
        HashMap<VolumeNumber, Map<Integer, Boolean>> ret = new HashMap<>();
        for (DrbdVolume drbdVlm : volumesMapRef.values())
        {
            Map<Integer, Boolean> peers = ret.get(drbdVlm.volId);
            if (peers == null)
            {
                peers = new HashMap<>();
                ret.put(drbdVlm.volId, peers);
            }
            for (DrbdConnection drbdCon : drbdVlm.getResource().getConnectionsMap().values())
            {
                peers.put(drbdCon.peerNodeId, drbdCon.getState() == DrbdConnection.State.CONNECTED);
            }
        }

        return ret;
    }

    private boolean peerVolumeUpToDate(DrbdConnection connection, VolumeNumber volumeNumber)
    {
        DrbdVolume peerVolume = connection.getVolume(volumeNumber);
        return peerVolume != null && peerVolume.getDiskState() == DiskState.UP_TO_DATE;
    }

    @Override
    public void connectionStateChanged(
        DrbdResource resource, DrbdConnection connection, DrbdConnection.State previous, DrbdConnection.State current
    )
    {
        try
        {
            connectionStateEvent.get().triggerEvent(
                ObjectIdentifier.connection(null, new NodeName(connection.getConnectionName()), resource.getResName()),
                current.toString());
        }
        catch (InvalidNameException ignored)
        {
        }
    }

    @Override
    public void connectionDestroyed(DrbdResource resource, DrbdConnection connection)
    {
        try
        {
            connectionStateEvent.get().closeStream(
                ObjectIdentifier.connection(null, new NodeName(connection.getConnectionName()), resource.getResName())
            );
        }
        catch (InvalidNameException ignored)
        {
        }
    }
}
