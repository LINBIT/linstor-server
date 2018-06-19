package com.linbit.linstor.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.DrbdStateChange;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Publishes DRBD events as LinStor events.
 */
@Singleton
public class DrbdEventPublisher implements SystemService, ResourceObserver, DrbdStateChange
{
    private static final ServiceName SERVICE_NAME;
    private static final String INSTANCE_PREFIX = "DrbdEventPublisher-";
    private static final String SERVICE_INFO = "DrbdEventPublisher";
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);

    private final ErrorReporter errorReporter;
    private final DrbdEventService drbdEventService;
    private final EventBroker eventBroker;

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
        ErrorReporter errorReporterRef,
        DrbdEventService drbdEventServiceRef,
        EventBroker eventBrokerRef
    )
    {
        errorReporter = errorReporterRef;
        drbdEventService = drbdEventServiceRef;
        eventBroker = eventBrokerRef;

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
        drbdEventService.addDrbdStateChangeObserver(this);
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
            eventBroker.openEventStream(resourceStateEventIdentifier(resource));
        }
    }

    @Override
    public void resourceDestroyed(DrbdResource resource)
    {
        if (resource.isKnownByLinstor())
        {
            eventBroker.closeEventStream(resourceStateEventIdentifier(resource));
        }
    }

    @Override
    public void volumeCreated(
        DrbdResource resource, DrbdConnection connection, DrbdVolume volume
    )
    {
        if (connection == null && resource.isKnownByLinstor())
        {
            eventBroker.openEventStream(volumeDiskStateEventIdentifier(resource, volume));
        }
    }

    @Override
    public void volumeDestroyed(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume
    )
    {
        if (resource.isKnownByLinstor())
        {
            eventBroker.closeEventStream(volumeDiskStateEventIdentifier(resource, volume));
        }
    }

    @Override
    public void drbdStateUnavailable()
    {
        eventBroker.closeAllEventStreams(
            ApiConsts.EVENT_VOLUME_DISK_STATE,
            new ObjectIdentifier(null, null, null, null)
        );
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
            eventBroker.triggerEvent(volumeDiskStateEventIdentifier(resource, volume));
            eventBroker.triggerEvent(resourceStateEventIdentifier(resource));
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
            eventBroker.triggerEvent(resourceStateEventIdentifier(resource));
        }
    }

    @Override
    public void roleChanged(DrbdResource resource, DrbdResource.Role previous, DrbdResource.Role current)
    {
        if (resource.isKnownByLinstor())
        {
            eventBroker.triggerEvent(resourceStateEventIdentifier(resource));
        }
    }

    private EventIdentifier volumeDiskStateEventIdentifier(DrbdResource resource, DrbdVolume volume)
    {
        return EventIdentifier.volumeDefinition(
            ApiConsts.EVENT_VOLUME_DISK_STATE,
            resource.getResName(),
            volume.getVolNr()
        );
    }

    private EventIdentifier resourceStateEventIdentifier(DrbdResource resource)
    {
        return EventIdentifier.resourceDefinition(
            ApiConsts.EVENT_RESOURCE_STATE,
            resource.getResName()
        );
    }
}
