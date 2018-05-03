package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.netcom.Peer;
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
    private final EventBroker eventBroker;
    private final Peer peer;

    @Inject
    public ResourceDeploymentStateEventHandler(
        EventBroker eventBrokerRef,
        Peer peerRef
    )
    {
        eventBroker = eventBrokerRef;
        peer = peerRef;
    }

    @Override
    public void execute(String eventAction, EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        if (eventAction.equals(ApiConsts.EVENT_STREAM_OPEN) || eventAction.equals(ApiConsts.EVENT_STREAM_VALUE))
        {
            ApiCallRcImpl deploymentState = new ApiCallRcImpl();

            EventRscDeploymentStateOuterClass.EventRscDeploymentState eventRscDeploymentState =
                EventRscDeploymentStateOuterClass.EventRscDeploymentState.parseDelimitedFrom(eventDataIn);

            for (MsgApiCallResponseOuterClass.MsgApiCallResponse apiCallResponse :
                 eventRscDeploymentState.getResponsesList())
            {
                ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
                entry.setReturnCode(apiCallResponse.getRetCode());
                entry.setMessageFormat(decorateMessageWithPeerInfo(apiCallResponse.getMessageFormat()));
                entry.setCauseFormat(apiCallResponse.getCauseFormat());
                entry.setCorrectionFormat(apiCallResponse.getCorrectionFormat());
                entry.setDetailsFormat(apiCallResponse.getDetailsFormat());
                entry.putAllObjRef(readLinStorMap(apiCallResponse.getObjRefsList()));
                entry.putAllVariables(readLinStorMap(apiCallResponse.getVariablesList()));
                deploymentState.addEntry(entry);
            }

            peer.getSatelliteState().setOnResource(
                eventIdentifier.getResourceName(),
                SatelliteResourceState::setDeploymentState,
                deploymentState
            );
        }
        else
        {
            peer.getSatelliteState().unsetOnResource(
                eventIdentifier.getResourceName(),
                SatelliteResourceState::setDeploymentState
            );
        }

        eventBroker.forwardEvent(new EventIdentifier(
            ApiConsts.EVENT_RESOURCE_DEPLOYMENT_STATE,
            eventIdentifier.getNodeName(),
            eventIdentifier.getResourceName(),
            null
        ), eventAction);
    }

    private Map<String, String> readLinStorMap(List<LinStorMapEntryOuterClass.LinStorMapEntry> linStorMap)
    {
        return linStorMap.stream()
            .collect(Collectors.toMap(
                LinStorMapEntryOuterClass.LinStorMapEntry::getKey,
                LinStorMapEntryOuterClass.LinStorMapEntry::getValue
            ));
    }

    private String decorateMessageWithPeerInfo(String message)
    {
        return "(" + peer.getNode().getName().displayValue + ") " + message;
    }
}
