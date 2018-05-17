package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.ResourceDefinitionEventStreamTracker;
import com.linbit.linstor.event.handler.SnapshotStateMachine;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscDeploymentStateOuterClass;
import com.linbit.linstor.satellitestate.SatelliteResourceState;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE
)
public class ResourceDeploymentStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final EventBroker eventBroker;
    private final ResourceDefinitionEventStreamTracker resourceDefinitionEventStreamTracker;
    private final SnapshotStateMachine snapshotStateMachine;

    @Inject
    public ResourceDeploymentStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        EventBroker eventBrokerRef,
        ResourceDefinitionEventStreamTracker resourceDefinitionEventStreamTrackerRef,
        SnapshotStateMachine snapshotStateMachineRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        eventBroker = eventBrokerRef;
        resourceDefinitionEventStreamTracker = resourceDefinitionEventStreamTrackerRef;
        snapshotStateMachine = snapshotStateMachineRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        boolean deploymentSuccess = false;

        if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN) ||
            eventAction.equals(ApiConsts.EVENT_STREAM_VALUE) ||
            eventAction.equals(ApiConsts.EVENT_STREAM_CLOSE_REMOVED))
        {
            ApiCallRcImpl deploymentState = new ApiCallRcImpl();

            EventRscDeploymentStateOuterClass.EventRscDeploymentState eventRscDeploymentState =
                EventRscDeploymentStateOuterClass.EventRscDeploymentState.parseDelimitedFrom(eventDataIn);

            for (MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse :
                 eventRscDeploymentState.getResponsesList())
            {
                ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
                entry.setReturnCode(apiCallResponse.getRetCode());
                entry.setMessageFormat(
                    "(" + eventIdentifier.getNodeName().displayValue + ") " + apiCallResponse.getMessageFormat()
                );
                entry.setCauseFormat(apiCallResponse.getCauseFormat());
                entry.setCorrectionFormat(apiCallResponse.getCorrectionFormat());
                entry.setDetailsFormat(apiCallResponse.getDetailsFormat());
                entry.putAllObjRef(readLinStorMap(apiCallResponse.getObjRefsList()));
                entry.putAllVariables(readLinStorMap(apiCallResponse.getVariablesList()));
                deploymentState.addEntry(entry);
            }

            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.setOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setDeploymentState,
                    deploymentState
                )
            );

            if (!eventAction.equals(ApiConsts.EVENT_STREAM_CLOSE_REMOVED) && !ApiRcUtils.isError(deploymentState))
            {
                deploymentSuccess = true;
            }
        }
        else
        {
            satelliteStateHelper.onSatelliteState(
                eventIdentifier.getNodeName(),
                satelliteState -> satelliteState.unsetOnResource(
                    eventIdentifier.getResourceName(),
                    SatelliteResourceState::setDeploymentState
                )
            );
        }

        if (!deploymentSuccess)
        {
            snapshotStateMachine.stepResourceSnapshots(eventIdentifier, true);
        }

        eventBroker.forwardEvent(eventIdentifier, eventAction);

        resourceDefinitionEventStreamTracker.resourceEventReceived(eventIdentifier, eventAction);
    }

    private Map<String, String> readLinStorMap(List<LinStorMapEntryOuterClass.LinStorMapEntry> linStorMap)
    {
        return linStorMap.stream()
            .collect(Collectors.toMap(
                LinStorMapEntryOuterClass.LinStorMapEntry::getKey,
                LinStorMapEntryOuterClass.LinStorMapEntry::getValue
            ));
    }
}
