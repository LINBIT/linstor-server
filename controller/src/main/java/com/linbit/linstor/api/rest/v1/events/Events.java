package com.linbit.linstor.api.rest.v1.events;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.SseFeature;

@Path("v1/events")
public class Events {
    private final EventHandlerBridge eventHandlerBridge;

    @Inject
    public Events(
        EventHandlerBridge eventHandlerBridgeRef
    )
    {
        eventHandlerBridge = eventHandlerBridgeRef;
    }

    @GET
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    @Path("drbd/promotion")
    public EventOutput promotionEvents() {
        final EventOutput eventOutput = new EventOutput();
        eventHandlerBridge.registerResourceClient(eventOutput);
        return eventOutput;
    }
}
