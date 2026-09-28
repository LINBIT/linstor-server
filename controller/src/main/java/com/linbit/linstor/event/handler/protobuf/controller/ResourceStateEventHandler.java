package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.rest.v1.events.EventDrbdHandlerBridge;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlMinIoSizeHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.event.common.ResourceStateEvent;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass.EventRscState;
import com.linbit.linstor.proto.eventdata.EventRscStateOuterClass.PeerState;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

@ProtobufEventHandler(
    eventName = InternalApiConsts.EVENT_RESOURCE_STATE
)
@Singleton
public class ResourceStateEventHandler implements EventHandler
{
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final SatelliteStateHelper satelliteStateHelper;
    private final ResourceStateEvent resourceStateEvent;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final LockGuardFactory lockGuardFactory;
    private final AutoDiskfulTask autoDiskfulTask;
    private final EventDrbdHandlerBridge eventDrbdHandlerBridge;
    private final CtrlMinIoSizeHelper ctrlMinIoSizeHelper;

    @Inject
    public ResourceStateEventHandler(
        CtrlTransactionHelper ctrlTransactionHelperRef,
        SatelliteStateHelper satelliteStateHelperRef,
        ResourceStateEvent resourceStateEventRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        LockGuardFactory lockGuardFactoryRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        EventDrbdHandlerBridge eventDrbdHandlerBridgeRef,
        ErrorReporter errorReporterRef,
        CtrlMinIoSizeHelper ctrlMinIoSizeHelperRef
    )
    {
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        satelliteStateHelper = satelliteStateHelperRef;
        resourceStateEvent = resourceStateEventRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        lockGuardFactory = lockGuardFactoryRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        eventDrbdHandlerBridge = eventDrbdHandlerBridgeRef;
        errorReporter = errorReporterRef;
        ctrlMinIoSizeHelper = ctrlMinIoSizeHelperRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {

        if (eventAction.equals(InternalApiConsts.EVENT_STREAM_VALUE))
        {
            EventRscStateOuterClass.EventRscState eventRscState =
                EventRscStateOuterClass.EventRscState.parseDelimitedFrom(eventDataIn);

            Boolean inUse = switch (eventRscState.getInUse())
            {
                case FALSE -> false;
                case TRUE -> true;
                case UNKNOWN -> null;
                default -> throw new ImplementationError("Unexpected proto InUse enum: " + eventRscState.getInUse());
            };

            final @Nullable Boolean open = eventRscState.hasOpen() ? eventRscState.getOpen() : null;
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState ->
                {
                    satelliteState.setOnResource(
                        eventIdentifier.getResourceName(),
                        SatelliteResourceState::setInUse,
                        inUse
                    );
                    satelliteState.setOnResource(
                        eventIdentifier.getResourceName(),
                        SatelliteResourceState::setOpen,
                        open
                    );
                }
            );

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setIsReady,
                    eventRscState.getReady()
                )
            );
            Map<VolumeNumber, Map<Integer/* nodeId */, Boolean/* connected */>> peersConnected = new HashMap<>();
            Map<Integer, PeerState> protoPeersConnected = eventRscState.getPeersConnectedMap();
            try
            {
                for (Map.Entry<Integer, PeerState> entry : protoPeersConnected.entrySet())
                {
                    peersConnected.put(
                        new VolumeNumber(entry.getKey()),
                        new HashMap<>(entry.getValue().getPeerNodeIdMap())
                    );
                }
            }
            catch (ValueOutOfRangeException exc)
            {
                throw new ImplementationError(exc);
            }
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setPeersConnected,
                    peersConnected
                )
            );

            final Integer promotionScore = eventRscState.hasPromotionScore() ?
                eventRscState.getPromotionScore() : null;
            final Boolean mayPromote = eventRscState.hasMayPromote() ?
                eventRscState.getMayPromote() : null;

            ResourceState resourceState = new ResourceState(
                eventRscState.getReady(),
                extractConnectedPeerStates(eventRscState),
                inUse,
                eventRscState.getUpToDate(),
                promotionScore,
                mayPromote,
                open
            );

            processEvent(eventIdentifier, inUse);
            processEventUpdateVolatile(eventIdentifier, promotionScore, mayPromote);
            resourceStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction, resourceState);
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState ->
                {
                    satelliteState.unsetOnResource(
                        eventIdentifier.getResourceName(),
                        SatelliteResourceState::setInUse
                    );
                    satelliteState.unsetOnResource(
                        eventIdentifier.getResourceName(),
                        SatelliteResourceState::setOpen
                    );
                }
            );

            processEventUpdateVolatile(eventIdentifier, null, null);
            resourceStateEvent.get().forwardEvent(eventIdentifier.getObjectIdentifier(), eventAction);
        }
    }

    private Map<VolumeNumber, Map<Integer, Boolean>> extractConnectedPeerStates(EventRscState eventRscStateRef)
    {
        Map<VolumeNumber, Map<Integer, Boolean>> vlmNrToConnectedPeerStates = new HashMap<>();
        try
        {
            for (Entry<Integer, PeerState> entry : eventRscStateRef.getPeersConnectedMap().entrySet())
            {
                vlmNrToConnectedPeerStates.put(
                    new VolumeNumber(entry.getKey()),
                    new HashMap<>(entry.getValue().getPeerNodeIdMap())
                );
            }
        }
        catch (ValueOutOfRangeException exc)
            {
            throw new ImplementationError(exc);
        }
        return vlmNrToConnectedPeerStates;
    }

    private void processEvent(EventIdentifier eventIdentifierRef, @Nullable Boolean inUseRef)
    {
        NodeName nodeName = eventIdentifierRef.getNodeName();
        ResourceName resourceName = eventIdentifierRef.getResourceName();

        // EventProcessor has already taken write lock on NodesMap
        try (LockGuard ignored = lockGuardFactory.build(LockType.WRITE, LockObj.RSC_DFN_MAP))
        {
            @Nullable ResourceDefinition rscDfn = null;
            @Nullable Resource rsc = ctrlApiDataLoader.loadRscOrNull(nodeName, resourceName);
            if (rsc != null && !rsc.isDeleted())
            {
                rscDfn = rsc.getResourceDefinition();
                StateFlags<Flags> flags = rsc.getStateFlags();
                if (inUseRef != null && inUseRef)
                {
                    if (flags.isSet(Resource.Flags.TIE_BREAKER))
                    {
                        flags.disableFlags(Flags.TIE_BREAKER);
                        flags.enableFlags(Resource.Flags.DRBD_DISKLESS);
                    }
                }
                if (flags.isSet(Resource.Flags.DRBD_DISKLESS))
                {
                    autoDiskfulTask.update(rsc);
                }
            }
            if (rscDfn == null)
            {
                rscDfn = ctrlApiDataLoader.loadRscDfnOrNull(resourceName);
            }
            if (rscDfn != null && Boolean.TRUE.equals(inUseRef))
            {
                Iterator<VolumeDefinition> vlmDfnIt = rscDfn.iterateVolumeDfn();
                while (vlmDfnIt.hasNext())
                {
                    VolumeDefinition vlmDfn = vlmDfnIt.next();
                    if (ctrlMinIoSizeHelper.isAutoMinIoSize(vlmDfn))
                    {
                        Props vlmDfnProps = vlmDfn.getProps();
                        @Nullable String freezeValue = vlmDfnProps.getProp(
                            ApiConsts.KEY_DRBD_FREEZE_BLOCK_SIZE,
                            ApiConsts.NAMESPC_LINSTOR_DRBD
                        );
                        if (!Boolean.parseBoolean(freezeValue))
                        {
                            vlmDfnProps.setProp(
                                ApiConsts.KEY_DRBD_FREEZE_BLOCK_SIZE,
                                "True",
                                ApiConsts.NAMESPC_LINSTOR_DRBD
                            );
                        }
                    }
                }
            }
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        ctrlTransactionHelper.commit();
    }

    private void processEventUpdateVolatile(
        EventIdentifier eventIdentifierRef,
        @Nullable Integer promotionScore,
        @Nullable Boolean mayPromote
    )
    {
        NodeName nodeName = eventIdentifierRef.getNodeName();
        ResourceName resourceName = eventIdentifierRef.getResourceName();

        // EventProcessor has already taken write lock on NodesMap
        try (LockGuard ignored = lockGuardFactory.build(LockType.WRITE, LockObj.RSC_DFN_MAP))
        {
            @Nullable Resource rsc = ctrlApiDataLoader.loadRscOrNull(nodeName, resourceName);

            if (rsc != null)
            {
                Set<AbsRscLayerObject<Resource>> drbdDataSet = LayerRscUtils.getRscDataByLayer(
                    rsc.getLayerData(), DeviceLayerKind.DRBD);
                for (AbsRscLayerObject<Resource> rlo : drbdDataSet)
                {
                    DrbdRscData<Resource> drbdRscData = ((DrbdRscData<Resource>) rlo);
                    drbdRscData.setPromotionScore(promotionScore);
                    if (!Objects.equals(drbdRscData.mayPromote(), mayPromote))
                    {
                        eventDrbdHandlerBridge.triggerMayPromote(rsc.getApiData(null, null, null), mayPromote);
                    }
                    drbdRscData.setMayPromote(mayPromote);
                }
            }
            else
            {
                errorReporter.logWarning("Event update for unknown resource %s on node %s", resourceName, nodeName);
            }
        }
    }
}
