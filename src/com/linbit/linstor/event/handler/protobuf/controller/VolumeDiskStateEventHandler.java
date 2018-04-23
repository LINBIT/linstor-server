package com.linbit.linstor.event.handler.protobuf.controller;

import com.linbit.linstor.ResourceName;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.ResourceState;
import com.linbit.linstor.api.pojo.VolumeState;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.event.handler.protobuf.ProtobufEventHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.eventdata.EventVlmDiskStateOuterClass;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@ProtobufEventHandler(
    eventName = ApiConsts.EVENT_VOLUME_DISK_STATE
)
public class VolumeDiskStateEventHandler implements EventHandler
{
    private final EventBroker eventBroker;
    private final Peer peer;

    @Inject
    public VolumeDiskStateEventHandler(
        EventBroker eventBrokerRef,
        Peer peerRef
    )
    {
        eventBroker = eventBrokerRef;
        peer = peerRef;
    }

    @Override
    public void execute(EventIdentifier eventIdentifier, InputStream eventDataIn)
        throws IOException
    {
        EventVlmDiskStateOuterClass.EventVlmDiskState eventVlmDiskState =
            EventVlmDiskStateOuterClass.EventVlmDiskState.parseDelimitedFrom(eventDataIn);

        if (peer.getResourceStates() == null)
        {
            peer.setResourceStates(new HashMap<>());
        }
        Map<ResourceName, ResourceState> resourceStates = peer.getResourceStates();

        if (resourceStates.get(eventIdentifier.getResourceName()) == null)
        {
            ResourceState resourceState = new ResourceState();
            resourceState.setNodeName(eventIdentifier.getNodeName().displayValue);
            resourceState.setRscName(eventIdentifier.getResourceName().displayValue);
            resourceState.setVolumes(new HashMap<>());
            resourceStates.put(eventIdentifier.getResourceName(), resourceState);
        }
        ResourceState resourceState = resourceStates.get(eventIdentifier.getResourceName());

        if (resourceState.getVolumeState(eventIdentifier.getVolumeNumber()) == null)
        {
            VolumeState volumeState = new VolumeState();
            volumeState.setVlmNr(eventIdentifier.getVolumeNumber());
            resourceState.putVolumeState(eventIdentifier.getVolumeNumber(), volumeState);
        }
        VolumeState volumeState = resourceState.getVolumeState(eventIdentifier.getVolumeNumber());

        volumeState.setDiskState(eventVlmDiskState.getDiskState());

        eventBroker.triggerEvent(eventIdentifier);
        eventBroker.triggerEvent(new EventIdentifier(
            ApiConsts.EVENT_RESOURCE_STATE,
            eventIdentifier.getNodeName(),
            eventIdentifier.getResourceName(),
            null
        ));
    }
}
