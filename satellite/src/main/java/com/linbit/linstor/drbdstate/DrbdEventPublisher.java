package com.linbit.linstor.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.VolumeDiskStateEvent;
import com.linbit.linstor.event.common.UsageState;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
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
        EventBroker eventBrokerRef,
        ResourceStateEvent resourceStateEventRef,
        VolumeDiskStateEvent volumeDiskStateEventRef
    )
    {
        drbdEventService = drbdEventServiceRef;
        resourceStateEvent = resourceStateEventRef;
        volumeDiskStateEvent = volumeDiskStateEventRef;

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
            volumeDiskStateEvent.get().closeStream(
                ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()));
        }
    }

    @Override
    public void diskStateChanged(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume,
        DrbdVolume.DiskState previous,
        DrbdVolume.DiskState current
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
        DrbdVolume.ReplState previous,
        DrbdVolume.ReplState current
    )
    {
        if (resource.isKnownByLinstor())
        {
            triggerResourceStateEvent(resource);
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
            determineUsageState(resource)
        );
    }

    private void triggerVolumeDiskStateEvent(DrbdResource resource, DrbdVolume volume)
    {
        volumeDiskStateEvent.get().triggerEvent(
            ObjectIdentifier.volumeDefinition(resource.getResName(), volume.getVolNr()),
            volume.diskStateInfo()
        );
    }

    private UsageState determineUsageState(DrbdResource drbdResource)
    {
        Map<VolumeNumber, DrbdVolume> volumesMap = drbdResource.getVolumesMap();

        return new UsageState(
            !volumesMap.isEmpty() && volumesMap.values().stream().allMatch(this::volumeReady),
            drbdResource.getRole() == DrbdResource.Role.PRIMARY,
            volumesMap.values().stream().map(DrbdVolume::getDiskState).allMatch(DrbdVolume.DiskState.UP_TO_DATE::equals)
        );
    }

    private boolean volumeReady(DrbdVolume volume)
    {
        boolean accessUpToDateData;

        if (volume.getDiskState() == DrbdVolume.DiskState.UP_TO_DATE)
        {
            accessUpToDateData = true;
        }
        else
        {
            accessUpToDateData = volume.getResource().getConnectionsMap().values().stream()
                .anyMatch(drbdConnection -> peerVolumeUpToDate(drbdConnection, volume.getVolNr()));
        }

        boolean connectionsEstablished = volume.getResource().getConnectionsMap().values().stream()
            .allMatch(drbdConnection -> drbdConnection.getState() == DrbdConnection.State.CONNECTED);

        return accessUpToDateData && connectionsEstablished;
    }

    private boolean peerVolumeUpToDate(DrbdConnection connection, VolumeNumber volumeNumber)
    {
        DrbdVolume peerVolume = connection.getVolume(volumeNumber);
        return peerVolume != null && peerVolume.getDiskState() == DrbdVolume.DiskState.UP_TO_DATE;
    }
}
