package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufEventHandler(
    eventName = InternalApiConsts.EVENT_RESOURCE_STATE
)
@Singleton
public class ResourceStateEventHandler implements EventHandler
{
    private final AccessContext apiCtx;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final SatelliteStateHelper satelliteStateHelper;
    private final ResourceStateEvent resourceStateEvent;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final AutoDiskfulTask autoDiskfulTask;
    private final StltConfigAccessor stltConfigAccesor;
    private final ErrorReporter errorReporter;

    @Inject
    public ResourceStateEventHandler(
        @SystemContext AccessContext apiCtxRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        SatelliteStateHelper satelliteStateHelperRef,
        ResourceStateEvent resourceStateEventRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        LockGuardFactory lockGuardFactoryRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        StltConfigAccessor stltConfigAccesorRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCtx = apiCtxRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        satelliteStateHelper = satelliteStateHelperRef;
        resourceStateEvent = resourceStateEventRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        lockGuardFactory = lockGuardFactoryRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        stltConfigAccesor = stltConfigAccesorRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        UsageState usageState;

        if (eventAction.equals(InternalApiConsts.EVENT_STREAM_VALUE))
        {
            EventRscStateOuterClass.EventRscState eventRscState =
                EventRscStateOuterClass.EventRscState.parseDelimitedFrom(eventDataIn);

            Boolean inUse;
            switch (eventRscState.getInUse())
            {
                case FALSE:
                    inUse = false;
                    break;
                case TRUE:
                    inUse = true;
                    break;
                case UNKNOWN:
                    inUse = null;
                    break;
                default:
                    throw new ImplementationError("Unexpected proto InUse enum: " + eventRscState.getInUse());
            }

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setInUse,
                    inUse
                )
            );

            usageState = new UsageState(
                eventRscState.getReady(),
                inUse,
                eventRscState.getUpToDate()
            );

            processEvent(eventIdentifier, inUse);
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setInUse
                )
            );

            usageState = null;
        }

        resourceStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction, usageState);
    }

    private void processEvent(EventIdentifier eventIdentifierRef, Boolean inUseRef)
    {
        NodeName nodeName = eventIdentifierRef.getNodeName();
        ResourceName resourceName = eventIdentifierRef.getResourceName();

            // EventProcessor has already taken write lock on NodesMap
            try (LockGuard lg = lockGuardFactory.build(LockType.WRITE, LockObj.RSC_DFN_MAP))
            {
                Resource rsc = ctrlApiDataLoader.loadRsc(nodeName, resourceName, true);
                StateFlags<Flags> flags = rsc.getStateFlags();
                if (inUseRef != null && inUseRef)
                {
                    if (flags.isSet(apiCtx, Resource.Flags.TIE_BREAKER))
                    {
                        flags.disableFlags(apiCtx, Flags.TIE_BREAKER);
                        flags.enableFlags(apiCtx, Resource.Flags.DRBD_DISKLESS);
                    }
                }
                if (flags.isSet(apiCtx, Resource.Flags.DRBD_DISKLESS))
                {
                    autoDiskfulTask.update(rsc);
                }
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError("ApiCtx does not have enough privileges");
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
            ctrlTransactionHelper.commit();
    }
}
