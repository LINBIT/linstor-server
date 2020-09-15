package com.linbit.linstor.api.rest.v1.events;

import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;

@Singleton
public class EventHandlerBridge {
    private final ErrorReporter errorReporter;
    private final ArrayList<EventOutput> rscClients = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public EventHandlerBridge(
        ErrorReporter errorReporterRef
    )
    {
        errorReporter = errorReporterRef;
    }

    public void registerResourceClient(EventOutput eventOut)
    {
        synchronized (rscClients)
        {
            rscClients.add(eventOut);
        }
    }

    public void triggerMayPromote(final Resource rsc, final Boolean mayPromote)
    {
        final OutboundEvent.Builder eventBuilder = new OutboundEvent.Builder();
        eventBuilder.mediaType(MediaType.APPLICATION_JSON_TYPE);
        eventBuilder.name("may-promote-change");
        JsonGenTypes.EventMayPromoteChange eventMayPromoteChange = new JsonGenTypes.EventMayPromoteChange();
        eventMayPromoteChange.node_name = rsc.getNode().getName().displayValue;
        eventMayPromoteChange.resource_name = rsc.getResourceDefinition().getName().displayValue;
        eventMayPromoteChange.may_promote = mayPromote;
        try
        {
            eventBuilder.data(objectMapper.writeValueAsString(eventMayPromoteChange));
            final OutboundEvent event = eventBuilder.build();
            sendRscEvent(event);
        } catch (JsonProcessingException e)
        {
            errorReporter.reportError(e);
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
                } catch (IOException e)
                {
                    closeEvents.remove(eventOutput);
                }
            }

            for (EventOutput eventOutput : closeEvents)
            {
                try
                {
                    eventOutput.close();
                } catch (IOException ignored) {}
                rscClients.remove(eventOutput);
            }
        }
    }
}
