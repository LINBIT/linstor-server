package com.linbit.linstor.event;

import com.linbit.WorkQueue;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class EventBroker
{
    private final ErrorReporter errorReporter;
    private final WatchStore watchStore;
    private final WatchableObjectFinder watchableObjectFinder;
    private final CommonSerializer commonSerializer;
    private final WorkQueue workQueue;
    private final CoreModule.PeerMap peerMap;
    private final Map<String, EventWriter> eventWriters;
    private final Map<String, EventWriterDescriptor> eventWriterDescriptors;

    private final ReentrantLock watchStoreLock;

    @Inject
    public EventBroker(
        ErrorReporter errorReporterRef,
        WatchStore watchStoreRef,
        WatchableObjectFinder watchableObjectFinderRef,
        CommonSerializer commonSerializerRef,
        @Named(LinStorModule.EVENT_WRITER_WORKER_POOL_NAME) WorkQueue workQueueRef,
        CoreModule.PeerMap peerMapRef,
        Map<String, EventWriter> eventWritersRef,
        Map<String, EventWriterDescriptor> eventWriterDescriptorsRef
    )
    {
        errorReporter = errorReporterRef;
        watchStore = watchStoreRef;
        watchableObjectFinder = watchableObjectFinderRef;
        commonSerializer = commonSerializerRef;
        workQueue = workQueueRef;
        peerMap = peerMapRef;
        eventWriters = eventWritersRef;
        eventWriterDescriptors = eventWriterDescriptorsRef;

        watchStoreLock = new ReentrantLock();
    }

    /**
     * Add a watch and send initial state for all relevant events.
     * <p>
     * Requires the nodes-map and resource-definition-map read locks to be held.
     */
    public void createWatch(AccessContext accCtx, Watch watch)
        throws AccessDeniedException, LinStorDataAlreadyExistsException
    {
        // Find all affected objects of each type under this watch
        Map<WatchableObject, List<ObjectIdentifier>> objectsToWatch =
            watchableObjectFinder.findDescendantObjects(accCtx, watch.getEventIdentifier().getObjectIdentifier());

        watchStoreLock.lock();
        try
        {
            // Send all relevant events to all affected objects
            for (EventWriterDescriptor descriptor : eventWriterDescriptors.values())
            {
                List<ObjectIdentifier> objects = objectsToWatch.get(descriptor.getObjectType());
                EventWriter eventWriter = eventWriters.get(descriptor.getEventName());
                for (ObjectIdentifier objectIdentifier : objects)
                {
                    writeAndSend(
                        new EventIdentifier(descriptor.getEventName(), objectIdentifier.getNodeName(),
                            objectIdentifier.getResourceName(), objectIdentifier.getVolumeNumber()),
                        eventWriter,
                        Collections.singleton(watch)
                    );
                }
            }

            watchStore.addWatch(watch);
        }
        finally
        {
            watchStoreLock.unlock();
        }
    }

    public void removeWatchesForPeer(String peerId)
    {
        watchStoreLock.lock();
        try
        {
            watchStore.removeWatchesForPeer(peerId);
        }
        finally
        {
            watchStoreLock.unlock();
        }
    }

    public void triggerEvent(EventIdentifier eventIdentifier)
    {
        EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
        if (eventWriter == null)
        {
            errorReporter.logError("Cannot trigger unknown event '%s'", eventIdentifier.getEventName());
        }
        else
        {
            Collection<Watch> watches;

            watchStoreLock.lock();
            try
            {
                watches = watchStore.getWatchesForEvent(eventIdentifier);
            }
            finally
            {
                watchStoreLock.unlock();
            }

            if (!watches.isEmpty())
            {
                writeAndSend(eventIdentifier, eventWriter, watches);
            }
        }
    }

    private void writeAndSend(EventIdentifier eventIdentifier, EventWriter eventWriter, Collection<Watch> watches)
    {
        workQueue.submit(new EventSender(eventIdentifier, eventWriter, watches));
    }

    private class EventSender implements Runnable
    {
        private final EventIdentifier eventIdentifier;
        private final EventWriter eventWriter;
        private final Collection<Watch> watches;

        EventSender(EventIdentifier eventIdentifierRef, EventWriter eventWriterRef, Collection<Watch> watchesRef)
        {
            eventIdentifier = eventIdentifierRef;
            eventWriter = eventWriterRef;
            watches = watchesRef;
        }

        @Override
        public void run()
        {
            try
            {
                byte[] eventData = eventWriter.writeEvent(eventIdentifier.getObjectIdentifier());

                if (eventData != null)
                {
                    watches.forEach(watch -> sendEvent(watch, eventData));
                }
            }
            catch (Exception exc)
            {
                errorReporter.reportError(
                    exc,
                    null,
                    null,
                    "Failed to write event " + eventIdentifier.getEventName()
                );
            }
        }

        private void sendEvent(
            Watch watch,
            byte[] eventData
        )
        {
            Peer peer;
            synchronized (peerMap)
            {
                peer = peerMap.get(watch.getPeerId());
            }

            if (peer == null)
            {
                errorReporter.logWarning("Watch for unknown peer %s", watch.getPeerId());
            }
            else
            {
                byte[] eventHeaderBytes = commonSerializer.builder(ApiConsts.API_EVENT)
                    .event(watch.getPeerWatchId(), eventIdentifier)
                    .build();

                byte[] completeData = new byte[eventHeaderBytes.length + eventData.length];
                System.arraycopy(eventHeaderBytes, 0, completeData, 0, eventHeaderBytes.length);
                System.arraycopy(eventData, 0, completeData, eventHeaderBytes.length, eventData.length);

                peer.sendMessage(completeData);
            }
        }

    }
}
