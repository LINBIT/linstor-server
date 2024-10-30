package com.linbit.linstor.api.rest.v1.events;

import com.linbit.linstor.api.rest.v1.RequestHelper;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseFeature;

@Path("v1/events")
public class Events
{
    private final RequestHelper requestHelper;
    private final EventDrbdHandlerBridge eventDrbdHandlerBridge;
    private final EventNodeHandlerBridge eventNodeHandlerBridge;

    @Inject
    public Events(
        RequestHelper requestHelperRef,
        EventDrbdHandlerBridge eventDrbdHandlerBridgeRef,
        EventNodeHandlerBridge eventNodeHandlerBridgeRef
    )
    {
        requestHelper = requestHelperRef;
        eventDrbdHandlerBridge = eventDrbdHandlerBridgeRef;
        eventNodeHandlerBridge = eventNodeHandlerBridgeRef;
    }

    @GET
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    @Path("drbd/promotion")
    public EventOutput promotionEvents(
        @Context Request request,
        @HeaderParam("Last-Event-ID") String lastEventId) throws IOException
    {
        final EventOutput eventOutput = new EventOutput();
        Response resp = requestHelper.doInScope(
            "Events-drbd-promotion",
            request,
            () ->
            {
                eventDrbdHandlerBridge.registerResourceClient(
                    eventOutput, lastEventId != null && lastEventId.equals("current"));
                return null;
            },
            false
        );

        if (resp != null)
        {
            eventOutput.write(
                new OutboundEvent.Builder()
                    .name("error")
                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                    .data(resp.getEntity()).build());
            eventOutput.close();
        }
        return eventOutput;
    }

    @GET
    @Produces(SseFeature.SERVER_SENT_EVENTS)
    @Path("nodes")
    public EventOutput nodeEvents(
        @Context Request request) throws IOException
    {
        final EventOutput eventOutput = new EventOutput();
        Response resp = requestHelper.doInScope(
            "Events-nodes",
            request,
            () -> {
                eventNodeHandlerBridge.registerResourceClient(eventOutput);
                return null;
            },
            false
        );

        if (resp != null)
        {
            eventOutput.write(
                new OutboundEvent.Builder()
                    .name("error")
                    .mediaType(MediaType.APPLICATION_JSON_TYPE)
                    .data(resp.getEntity()).build());
            eventOutput.close();
        }
        return eventOutput;
    }
}
