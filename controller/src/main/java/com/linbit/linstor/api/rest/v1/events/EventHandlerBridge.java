package com.linbit.linstor.api.rest.v1.events;

import com.linbit.linstor.logging.ErrorReporter;

import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;

public abstract class EventHandlerBridge
{
    protected final ErrorReporter errorReporter;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ArrayList<EventOutput> rscClients = new ArrayList<>();

    protected EventHandlerBridge(ErrorReporter errorReporterRef)
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

    protected void sendEvent(String eventName, Object eventData)
    {
        ArrayList<EventOutput> closeEvents = new ArrayList<>();

        final OutboundEvent.Builder eventBuilder = new OutboundEvent.Builder();
        eventBuilder.mediaType(MediaType.APPLICATION_JSON_TYPE);
        eventBuilder.name(eventName);
        try
        {
            final String eventJsonData = objectMapper.writeValueAsString(eventData);

            final OutboundEvent event = eventBuilder.data(eventJsonData).build();
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
                        closeEvents.add(eventOutput);
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
        catch (JsonProcessingException exc)
        {
            errorReporter.reportError(exc);
        }
    }
}
