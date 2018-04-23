package com.linbit.linstor.api.protobuf.controller;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgEventOuterClass;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@ProtobufApiCall(
    name = ApiConsts.API_EVENT,
    description = "Handles an event"
)
public class IntEvent implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final Map<String, EventHandler> eventHandlers;

    @Inject
    public IntEvent(
        ErrorReporter errorReporterRef,
        Map<String, EventHandler> eventHandlersRef
    )
    {
        errorReporter = errorReporterRef;
        eventHandlers = eventHandlersRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgEventOuterClass.MsgEvent msgEvent = MsgEventOuterClass.MsgEvent.parseDelimitedFrom(msgDataIn);

        EventHandler eventHandler = eventHandlers.get(msgEvent.getEventName());
        if (eventHandler == null)
        {
            errorReporter.logWarning("Unknown event '%s' received", msgEvent.getEventName());
        }
        else
        {
            try
            {
                NodeName nodeName =
                    msgEvent.hasNodeName() ? new NodeName(msgEvent.getNodeName()) : null;
                ResourceName resourceName =
                    msgEvent.hasResourceName() ? new ResourceName(msgEvent.getResourceName()) : null;
                VolumeNumber volumeNumber =
                    msgEvent.hasVolumeNumber() ? new VolumeNumber(msgEvent.getVolumeNumber()) : null;

                eventHandler.execute(
                    new EventIdentifier(msgEvent.getEventName(), nodeName, resourceName, volumeNumber),
                    msgDataIn
                );
            }
            catch (InvalidNameException | ValueOutOfRangeException exc)
            {
                errorReporter.logWarning("Invalid event received: " + exc.getMessage());
            }
        }
    }
}
