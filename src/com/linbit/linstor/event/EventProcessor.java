package com.linbit.linstor.event;

import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Processes incoming events
 */
@Singleton
public class EventProcessor
{
    private final ErrorReporter errorReporter;
    private final Map<String, Provider<EventHandler>> eventHandlers;

    @Inject
    public EventProcessor(
        ErrorReporter errorReporterRef,
        Map<String, Provider<EventHandler>> eventHandlersRef
    )
    {
        errorReporter = errorReporterRef;
        eventHandlers = eventHandlersRef;
    }

    public void handleEvent(
        String eventAction,
        String eventName,
        String nodeNameStr,
        String resourceNameStr,
        Integer volumeNr,
        InputStream eventDataIn
    )
        throws IOException
    {
        Provider<EventHandler> eventHandlerProvider = eventHandlers.get(eventName);
        if (eventHandlerProvider == null)
        {
            errorReporter.logWarning("Unknown event '%s' received", eventName);
        }
        else
        {
            try
            {
                NodeName nodeName =
                    nodeNameStr != null ? new NodeName(nodeNameStr) : null;
                ResourceName resourceName =
                    resourceNameStr != null ? new ResourceName(resourceNameStr) : null;
                VolumeNumber volumeNumber =
                    volumeNr != null ? new VolumeNumber(volumeNr) : null;

                EventIdentifier eventIdentifier = new EventIdentifier(eventName, nodeName, resourceName, volumeNumber);

                eventHandlerProvider.get().execute(eventAction, eventIdentifier, eventDataIn);
            }
            catch (InvalidNameException | ValueOutOfRangeException exc)
            {
                errorReporter.logWarning("Invalid event received: " + exc.getMessage());
            }
        }
    }
}
