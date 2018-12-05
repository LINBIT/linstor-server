package com.linbit.linstor.event;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.Node;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.event.handler.EventHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionMgrGenerator;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Processes incoming events
 */
@Singleton
public class EventProcessor
{
    private final ErrorReporter errorReporter;
    private final Map<String, Provider<EventHandler>> eventHandlers;
    private final LinStorScope apiCallScope;
    private final TransactionMgrGenerator transactionMgrGenerator;
    private final Provider<TransactionMgr> transMgrProvider;

    // Synchronizes access to incomingEventStreamStore and pendingEventsPerPeer
    private final ReentrantLock eventHandlingLock;

    private final EventStreamStore incomingEventStreamStore;

    @Inject
    public EventProcessor(
        ErrorReporter errorReporterRef,
        Map<String, Provider<EventHandler>> eventHandlersRef,
        LinStorScope apiCallScopeRef,
        TransactionMgrGenerator transactionMgrGeneratorRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        eventHandlers = eventHandlersRef;
        apiCallScope = apiCallScopeRef;
        transactionMgrGenerator = transactionMgrGeneratorRef;
        transMgrProvider = transMgrProviderRef;

        eventHandlingLock = new ReentrantLock();
        incomingEventStreamStore = new EventStreamStoreImpl();
    }

    public void connectionClosed(Peer peer)
    {
        eventHandlingLock.lock();
        try
        {
            Node node = peer.getNode();
            if (node != null && !node.isDeleted())
            {
                // The peer is a Satellite
                for (Map.Entry<String, Provider<EventHandler>> eventHandlerEntry : eventHandlers.entrySet())
                {
                    Collection<EventIdentifier> eventStreams = incomingEventStreamStore.getDescendantEventStreams(
                        EventIdentifier.node(eventHandlerEntry.getKey(), node.getName())
                    );

                    for (EventIdentifier eventIdentifier : eventStreams)
                    {
                        executeNoConnection(eventHandlerEntry.getValue(), eventIdentifier, peer);

                        incomingEventStreamStore.removeEventStream(eventIdentifier);
                    }
                }
            }
        }
        finally
        {
            eventHandlingLock.unlock();
        }
    }

    public void handleEvent(
        String eventAction,
        String eventName,
        String resourceNameStr,
        Integer volumeNr,
        String snapshotNameStr,
        Peer peer,
        InputStream eventDataIn
    )
    {
        eventHandlingLock.lock();
        try
        {
            Provider<EventHandler> eventHandlerProvider = eventHandlers.get(eventName);
            if (eventHandlerProvider == null)
            {
                errorReporter.logWarning("Unknown event '%s' received", eventName);
            }
            else
            {
                ResourceName resourceName =
                    resourceNameStr != null ? new ResourceName(resourceNameStr) : null;
                VolumeNumber volumeNumber =
                    volumeNr != null ? new VolumeNumber(volumeNr) : null;
                SnapshotName snapshotName =
                    snapshotNameStr != null ? new SnapshotName(snapshotNameStr) : null;

                EventIdentifier eventIdentifier = new EventIdentifier(eventName, new ObjectIdentifier(
                    peer.getNode().getName(), resourceName, volumeNumber, snapshotName
                ));

                incomingEventStreamStore.addEventStreamIfNew(eventIdentifier);

                errorReporter.logTrace("Peer %s, event '%s' %s", peer, eventIdentifier, eventAction);
                eventHandlerProvider.get().execute(eventAction, eventIdentifier, eventDataIn);

                if (eventAction.equals(ApiConsts.EVENT_STREAM_CLOSE_REMOVED))
                {
                    incomingEventStreamStore.removeEventStream(eventIdentifier);
                }
            }

            transMgrProvider.get().commit();
        }
        catch (InvalidNameException | ValueOutOfRangeException exc)
        {
            errorReporter.logWarning("Invalid event received: " + exc.getMessage());
        }
        catch (Exception | ImplementationError exc)
        {
            errorReporter.reportError(exc);
        }
        finally
        {
            eventHandlingLock.unlock();
        }
    }

    private void executeNoConnection(
        Provider<EventHandler> eventHandler,
        EventIdentifier eventIdentifier,
        Peer peer
    )
    {
        TransactionMgr transMgr = transactionMgrGenerator.startTransaction();
        apiCallScope.enter();
        try
        {
            apiCallScope.seed(Peer.class, peer);
            apiCallScope.seed(TransactionMgr.class, transMgr);
            eventHandler.get().execute(
                ApiConsts.EVENT_STREAM_CLOSE_NO_CONNECTION, eventIdentifier, null);
            transMgr.commit();
        }
        catch (Exception exc)
        {
            errorReporter.reportError(exc, null, null,
                "Event handler for " + eventIdentifier + " failed on connection closed");
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
}
