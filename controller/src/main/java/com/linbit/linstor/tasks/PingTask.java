package com.linbit.linstor.tasks;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

@Singleton
public class PingTask implements Task
{
    private static final int PING_TIMEOUT = 5_000;
    private static final long PING_SLEEP = 1_000;

    private final Object syncObj = new Object();
    private final HashSet<Peer> peerSet = new HashSet<>();
    private final ErrorReporter errorReporter;
    private final ReconnectorTask reconnector;

    @Inject
    public PingTask(ErrorReporter errorReporterRef, ReconnectorTask reconnectorRef)
    {
        errorReporter = errorReporterRef;
        reconnector = reconnectorRef;

        reconnector.setPingTask(this);
    }

    public void add(Peer peer)
    {
        synchronized (syncObj)
        {
            peerSet.add(peer);
        }
    }

    public void remove(Peer peer)
    {
        synchronized (syncObj)
        {
            peerSet.remove(peer);
        }
    }

    @Override
    public long run(long scheduleAt)
    {
        final List<Peer> currentPeers;
        synchronized (syncObj)
        {
            currentPeers = new ArrayList<>(peerSet);
        }
        final List<Peer> peersToRemove = sendPingMessages(currentPeers);
        synchronized (syncObj)
        {
            peerSet.removeAll(peersToRemove);
        }
        return getNextFutureReschedule(scheduleAt, PING_SLEEP);
    }

    /**
     * Sends the ping message to the given list of Peers. If the peer's lastPongReceived is older than
     * {@value #PING_TIMEOUT} ms, attempt to reconnect to the peer
     *
     * @param currentPeers The list of peers to be pinged
     *
     * @return A list of peers that should be removed from the pinglist since we started a reconnect.
     * It should not cause issues if we send them new pings, it is simply unnecessary.
     */
    private @Nonnull List<Peer> sendPingMessages(@Nonnull final List<Peer> currentPeers)
    {
        List<Peer> peersToRemove = new ArrayList<>();
        for (final Peer peer : currentPeers)
        {
            final long lastPingReceived = peer.getLastPongReceived();
            final long lastPingSent = peer.getLastPingSent();
            boolean reconnect = false;
            if (!peer.isConnected(false) || lastPingReceived + PING_TIMEOUT < lastPingSent)
            {
                reconnect = true;
            }
            if (!reconnect)
            {
                try
                {
                    peer.sendPing();
                }
                catch (Exception exc)
                {
                    errorReporter.reportError(exc);
                    reconnect = true;
                }
            }
            if (reconnect)
            {
                errorReporter.logTrace(
                    "Connection to " + peer + " lost. Removed from pingList, added to reconnectList."
                );
                peersToRemove.add(peer);
                try
                {
                    reconnector.add(peer.getConnector().reconnect(peer), true, true);
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    errorReporter.reportError(ioExc);
                }
            }
        }
        return peersToRemove;
    }
}
