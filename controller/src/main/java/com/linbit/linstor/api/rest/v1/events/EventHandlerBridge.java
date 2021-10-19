package com.linbit.linstor.api.rest.v1.events;

import com.linbit.linstor.api.pojo.DrbdRscPojo;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.core.apis.ResourceApi;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;

@Singleton
public class EventHandlerBridge
{
    private final ErrorReporter errorReporter;
    private final ArrayList<EventOutput> rscClients = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CtrlApiCallHandler ctrlApiCallHandler;

    @Inject
    public EventHandlerBridge(
        ErrorReporter errorReporterRef,
        CtrlApiCallHandler ctrlApiCallHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        ctrlApiCallHandler = ctrlApiCallHandlerRef;
    }

    public void registerResourceClient(EventOutput eventOut, boolean sendInitialState)
    {
        synchronized (rscClients)
        {
            rscClients.add(eventOut);
        }

        if (sendInitialState)
        {
            ResourceList resourceList = ctrlApiCallHandler.listResource(
                Collections.emptyList(), Collections.emptyList());

            resourceList.getResources().forEach(
                rsc ->
                {
                    if (rsc.getLayerData().getLayerKind() == DeviceLayerKind.DRBD)
                    {
                        DrbdRscPojo drbdRscPojo = (DrbdRscPojo) rsc.getLayerData();
                        triggerMayPromote(rsc, drbdRscPojo.mayPromote());
                    }
                }
            );
        }
    }

    public void triggerMayPromote(final ResourceApi rsc, final Boolean mayPromote)
    {
        final OutboundEvent.Builder eventBuilder = new OutboundEvent.Builder();
        eventBuilder.mediaType(MediaType.APPLICATION_JSON_TYPE);
        eventBuilder.name("may-promote-change");
        JsonGenTypes.EventMayPromoteChange eventMayPromoteChange = new JsonGenTypes.EventMayPromoteChange();
        eventMayPromoteChange.node_name = rsc.getNodeName();
        eventMayPromoteChange.resource_name = rsc.getName();
        eventMayPromoteChange.may_promote = mayPromote;
        try
        {
            eventBuilder.data(objectMapper.writeValueAsString(eventMayPromoteChange));
            final OutboundEvent event = eventBuilder.build();
            sendRscEvent(event);
        }
        catch (JsonProcessingException exc)
        {
            errorReporter.reportError(exc);
        }
    }

    private void sendRscEvent(final OutboundEvent event)
    {
        ArrayList<EventOutput> closeEvents = new ArrayList<>();
        synchronized (rscClients)
        {
            for (EventOutput eventOutput : rscClients)
            {
                try
                {
                    eventOutput.write(event);
                }
                catch (IOException exc)
                {
                    closeEvents.remove(eventOutput);
                }
            }

            for (EventOutput eventOutput : closeEvents)
            {
                try
                {
                    eventOutput.close();
                }
                catch (IOException ignored)
                {
                }
                rscClients.remove(eventOutput);
            }
        }
    }
}
