package com.linbit.linstor.drbdstate;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.EventBroker;

import javax.inject.Inject;
import javax.inject.Singleton;
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
    private final ControllerPeerConnector controllerPeerConnector;
    private final EventBroker eventBroker;

    private ServiceName instanceName;
    private boolean started = false;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("DrbdEventService");
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Inject
    public DrbdEventPublisher(
        DrbdEventService drbdEventServiceRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        EventBroker eventBrokerRef
    )
    {
        drbdEventService = drbdEventServiceRef;
        controllerPeerConnector = controllerPeerConnectorRef;
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
    public void diskStateChanged(
        DrbdResource resource,
        DrbdConnection connection,
        DrbdVolume volume,
        DrbdVolume.DiskState previous,
        DrbdVolume.DiskState current
    )
    {
        NodeData localNode = controllerPeerConnector.getLocalNode();
        if (localNode != null)
        {
            eventBroker.triggerEvent(new EventIdentifier(
                ApiConsts.EVENT_VOLUME_DISK_STATE,
                localNode.getName(),
                resource.getName(),
                volume.getVolNr()
            ));
        }
    }
}
