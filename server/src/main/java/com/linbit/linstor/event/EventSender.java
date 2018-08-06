package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.api.interfaces.serializer.CommonSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.event.writer.EventWriter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Singleton
public class EventSender
{
    private final ErrorReporter errorReporter;
    private final WatchStore watchStore;
    private final CommonSerializer commonSerializer;
    private final CoreModule.PeerMap peerMap;
    private final Map<String, EventWriter> eventWriters;
    private final LinStorScope apiCallScope;
    private final Provider<TransactionMgr> transMgrGenerator;

    // Serialize event writing and sending.
    // This could be relaxed to allow events to be written on multiple threads, if required for performance.
    private final ReentrantLock watchAndStreamLock;
    private final EventStreamStore outgoingEventStreamStore;

    @Inject
    public EventSender(
        ErrorReporter errorReporterRef,
        WatchStore watchStoreRef,
        CommonSerializer commonSerializerRef,
        CoreModule.PeerMap peerMapRef,
        Map<String, EventWriter> eventWritersRef,
        LinStorScope apiCallScopeRef,
        @Named(LinStorModule.TRANS_MGR_GENERATOR) Provider<TransactionMgr> trnActProviderRef
    )
    {
        errorReporter = errorReporterRef;
        watchStore = watchStoreRef;
        commonSerializer = commonSerializerRef;
        peerMap = peerMapRef;
        eventWriters = eventWritersRef;
        apiCallScope = apiCallScopeRef;
        transMgrGenerator = trnActProviderRef;

        watchAndStreamLock = new ReentrantLock();
        outgoingEventStreamStore = new EventStreamStoreImpl();
    }

    public void createWatch(Watch watch)
    {
        watchAndStreamLock.lock();
        try
        {
            watchStore.addWatch(watch);

            Collection<String> eventNames = getMatchingEventNames(watch.getEventIdentifier().getEventName());

            for (String eventName : eventNames)
            {
                Collection<EventIdentifier> eventStreams =
                    outgoingEventStreamStore.getDescendantEventStreams(
                        new EventIdentifier(eventName, watch.getEventIdentifier().getObjectIdentifier())
                    );

                for (EventIdentifier eventIdentifier : eventStreams)
                {
                    EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
                    if (eventWriter == null)
                    {
                        errorReporter.logWarning("Stream is open for unknown event " + eventIdentifier.getEventName());
                    }
                    else
                    {
                        writeAndSendTo(
                            eventIdentifier,
                            ApiConsts.EVENT_STREAM_OPEN,
                            eventWriter,
                            Collections.singleton(watch)
                        );
                    }
                }
            }
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            errorReporter.logError(
                "Watch already exists for peer " + watch.getPeerId() + ", id " + watch.getPeerWatchId());
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void deleteWatch(String peerId, int peerWatchId)
    {
        watchAndStreamLock.lock();
        try
        {
            watchStore.removeWatchForPeerAndId(peerId, peerWatchId);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void connectionClosed(Peer peer)
    {
        watchAndStreamLock.lock();
        try
        {
            watchStore.removeWatchesForPeer(peer.getId());
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void openEventStream(EventIdentifier eventIdentifier)
    {
        watchAndStreamLock.lock();
        try
        {
            outgoingEventStreamStore.addEventStream(eventIdentifier);
            sendToWatches(eventIdentifier, ApiConsts.EVENT_STREAM_OPEN);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ImplementationError(
                "Stream is already open: " + eventIdentifier, exc);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void openOrTriggerEvent(EventIdentifier eventIdentifier)
    {
        boolean isNew;
        watchAndStreamLock.lock();
        try
        {
            isNew = outgoingEventStreamStore.addEventStreamIfNew(eventIdentifier);
            sendToWatches(eventIdentifier, isNew ? ApiConsts.EVENT_STREAM_OPEN : ApiConsts.EVENT_STREAM_VALUE);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void triggerEvent(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        watchAndStreamLock.lock();
        try
        {
            sendToWatches(eventIdentifier, eventStreamAction);
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void closeEventStream(EventIdentifier eventIdentifier, String eventStreamAction, boolean onlyIfOpen)
    {
        watchAndStreamLock.lock();
        try
        {
            boolean wasPresent = outgoingEventStreamStore.removeEventStream(eventIdentifier);

            if (!onlyIfOpen || wasPresent)
            {
                sendToWatches(eventIdentifier, eventStreamAction);
            }
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    public void closeAllEventStreams(String eventName, ObjectIdentifier objectIdentifier)
    {
        watchAndStreamLock.lock();
        try
        {
            Collection<EventIdentifier> eventStreams =
                outgoingEventStreamStore.getDescendantEventStreams(
                    new EventIdentifier(eventName, objectIdentifier));

            for (EventIdentifier eventIdentifier : eventStreams)
            {
                sendToWatches(eventIdentifier, ApiConsts.EVENT_STREAM_CLOSE_REMOVED);

                outgoingEventStreamStore.removeEventStream(eventIdentifier);
            }
        }
        finally
        {
            watchAndStreamLock.unlock();
        }
    }

    private void sendToWatches(EventIdentifier eventIdentifier, String eventStreamAction)
    {
        EventWriter eventWriter = eventWriters.get(eventIdentifier.getEventName());
        if (eventWriter == null)
        {
            errorReporter.logError("Cannot trigger unknown event '%s'", eventIdentifier.getEventName());
        }
        else
        {
            Collection<Watch> watches = watchStore.getWatchesForEvent(eventIdentifier);

            if (!watches.isEmpty())
            {
                writeAndSendTo(eventIdentifier, eventStreamAction, eventWriter, watches);
            }

            if (ApiConsts.EVENT_STREAM_CLOSE_REMOVED.equals(eventStreamAction) ||
                ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION.equals(eventStreamAction))
            {
                clearEventData(eventIdentifier, eventWriter);
            }
        }
    }

    private Collection<String> getMatchingEventNames(String eventName)
    {
        return eventName == null || eventName.isEmpty() ? eventWriters.keySet() : Collections.singleton(eventName);
    }

    private void writeAndSendTo(
        EventIdentifier eventIdentifier,
        String eventStreamAction,
        EventWriter eventWriter,
        Collection<Watch> watches
    )
    {
        byte[] eventData = writeEventData(eventIdentifier, eventWriter);

        byte[] dataToSend;
        if (ApiConsts.EVENT_STREAM_CLOSE_REMOVED.equals(eventStreamAction) ||
            ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION.equals(eventStreamAction))
        {
            // Always send close events even when the event writer doesn't produce any data
            dataToSend = eventData == null ? new byte[] {} : eventData;
        }
        else
        {
            dataToSend = eventData;
        }

        if (dataToSend != null)
        {
            watches.forEach(
                watch -> sendEvent(
                    watch,
                    eventIdentifier,
                    eventStreamAction,
                    dataToSend
                )
            );
        }
    }

    private byte[] writeEventData(
        EventIdentifier eventIdentifier,
        EventWriter eventWriter
    )
    {
        byte[] eventData = null;
        TransactionMgr transMgr = transMgrGenerator.get();
        apiCallScope.enter();
        try
        {
            apiCallScope.seed(TransactionMgr.class, transMgr);
            eventData = eventWriter.writeEvent(eventIdentifier.getObjectIdentifier());
            transMgr.commit();
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
        finally
        {
            try
            {
                transMgr.rollback();
            }
            catch (SQLException exc)
            {
                errorReporter.reportError(exc);
            }
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
            apiCallScope.exit();
        }
        return eventData;
    }

    private void clearEventData(
        EventIdentifier eventIdentifier,
        EventWriter eventWriter
    )
    {
        TransactionMgr transMgr = transMgrGenerator.get();
        apiCallScope.enter();
        try
        {
            apiCallScope.seed(TransactionMgr.class, transMgr);
            eventWriter.clear(eventIdentifier.getObjectIdentifier());
            transMgr.commit();
        }
        catch (Exception exc)
        {
            errorReporter.reportError(
                exc,
                null,
                null,
                "Failed to clear event " + eventIdentifier.getEventName()
            );
        }
        finally
        {
            try
            {
                transMgr.rollback();
            }
            catch (SQLException exc)
            {
                errorReporter.reportError(exc);
            }
            if (transMgr != null)
            {
                transMgr.returnConnection();
            }
            apiCallScope.exit();
        }
    }

    private void sendEvent(
        Watch watch,
        EventIdentifier eventIdentifier,
        String eventStreamAction,
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
            long eventCounter = watchStore.getAndIncrementEventCounter(watch);

            byte[] eventHeaderBytes = commonSerializer.onewayBuilder(ApiConsts.API_EVENT)
                .event(watch.getPeerWatchId(), eventCounter, eventIdentifier, eventStreamAction)
                .build();

            byte[] completeData = new byte[eventHeaderBytes.length + eventData.length];
            System.arraycopy(eventHeaderBytes, 0, completeData, 0, eventHeaderBytes.length);
            System.arraycopy(eventData, 0, completeData, eventHeaderBytes.length, eventData.length);

            peer.sendMessage(completeData);
        }
    }
}
