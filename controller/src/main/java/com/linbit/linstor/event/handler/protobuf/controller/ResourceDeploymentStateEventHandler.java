package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiRcUtils;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.common.ResourceDeploymentStateEvent;
import com.linbit.linstor.event.handler.SatelliteStateHelper;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.SnapshotStateMachine;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.MsgApiCallResponseOuterClass;
import com.linbit.linstor.proto.eventdata.EventRscDeploymentStateOuterClass;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE
)
@Singleton
public class ResourceDeploymentStateEventHandler implements EventHandler
{
    private final SatelliteStateHelper satelliteStateHelper;
    private final ResourceDeploymentStateEvent resourceDeploymentStateEvent;
    private final SnapshotStateMachine snapshotStateMachine;

    @Inject
    public ResourceDeploymentStateEventHandler(
        SatelliteStateHelper satelliteStateHelperRef,
        ResourceDeploymentStateEvent resourceDeploymentStateEventRef,
        SnapshotStateMachine snapshotStateMachineRef
    )
    {
        satelliteStateHelper = satelliteStateHelperRef;
        resourceDeploymentStateEvent = resourceDeploymentStateEventRef;
        snapshotStateMachine = snapshotStateMachineRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        boolean deploymentFailed = false;
        ApiCallRcImpl deploymentState;

        if (eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            deploymentState = new ApiCallRcImpl();

            EventRscDeploymentStateOuterClass.EventRscDeploymentState eventRscDeploymentState =
                EventRscDeploymentStateOuterClass.EventRscDeploymentState.parseDelimitedFrom(eventDataIn);

            for (MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse :
                 eventRscDeploymentState.getResponsesList())
            {
                ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
                entry.setReturnCode(apiCallResponse.getRetCode());
                entry.setMessage(
                    "(" + eventIdentifier.getNodeName().displayValue + ") " + apiCallResponse.getMessage()
                );
                entry.setCause(apiCallResponse.getCause());
                entry.setCorrection(apiCallResponse.getCorrection());
                entry.setDetails(apiCallResponse.getDetails());
                entry.putAllObjRef(readLinStorMap(apiCallResponse.getObjRefsList()));
                deploymentState.addEntry(entry);
            }

            if (ApiRcUtils.isError(deploymentState))
            {
                deploymentFailed = true;
            }
        }
        else
        {
            deploymentState = null;
        }

        if (deploymentFailed)
        {
            snapshotStateMachine.stepResourceSnapshots(
                eventIdentifier, deploymentState, eventAction.equals(ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION));
        }

        resourceDeploymentStateEvent.get().forwardEvent(
            eventIdentifier.getObjectIdentifier(), eventAction, deploymentState);
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
