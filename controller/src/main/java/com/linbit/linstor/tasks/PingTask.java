package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.TaskScheduleService.Task;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
    private final AccessContext sysCtx;
    private final LockGuardFactory lockGuardFactory;

    @Inject
    public PingTask(
        ErrorReporter errorReporterRef,
        @SystemContext AccessContext sysCtxRef,
        ReconnectorTask reconnectorRef,
        LockGuardFactory lockGuardFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        sysCtx = sysCtxRef;
        reconnector = reconnectorRef;
        lockGuardFactory = lockGuardFactoryRef;

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
     * @param peers The list of peers to be pinged
     *
     * @return A list of peers that should be removed from the pinglist since we started a reconnect.
     * It should not cause issues if we send them new pings, it is simply unnecessary.
     */
    private List<Peer> sendPingMessages(final List<Peer> peers)
    {
        List<Peer> peersToRemove = new ArrayList<>();
        HashMap<Peer, Peer> peersToCurrentPeers = getCurrentPeers(peers);

        for (final Peer peer : peers)
        {
            final long lastPingReceived = peer.getLastPongReceived();
            final long lastPingSent = peer.getLastPingSent();
            boolean reconnect = false;

            boolean isConnected = peer.isConnected(false);
            boolean allowReconnect = peer.isAllowReconnect();

            @Nullable Peer nodesCurrentPeer = peersToCurrentPeers.get(peer);
            if (nodesCurrentPeer == null)
            {
                errorReporter.logTrace("Dropping " + peer + " since node was deleted");
                isConnected = false;
                allowReconnect = false;
            }
            else if (peer != nodesCurrentPeer)
            {
                errorReporter.logTrace("Dropping " + peer + " in favor of " + nodesCurrentPeer);
                isConnected = false;
                allowReconnect = false;
            }

            if ((!isConnected || lastPingReceived + PING_TIMEOUT < lastPingSent) && allowReconnect)
            {
                reconnect = true;
            }
            if (!reconnect && isConnected)
            {
                try
                {
                    peer.sendPing();
                }
                catch (Exception exc)
                {
                    errorReporter.reportError(exc);
                    reconnect = allowReconnect;
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
            if (!isConnected && !allowReconnect)
            {
                peersToRemove.add(peer); // just to make sure this peer gets removed from our peerSet
            }
        }
        return peersToRemove;
    }

    /**
     * If a peer's node was already deleted or got reconnected (i.e. a new peer) that we just are about to process,
     * we need to take certain action. This method's returned map will contain all peers of the input list as keys.
     * There are only three different cases of the value:
     * <ul>
     *  <li>The value is identical to its key: the peer is still the same (default case)</li>
     *  <li>The value is a different peer than its key: the node got reconnected</li>
     *  <li>The value is <code>null</code> for the given key, the node is already deleted</li>
     * </ul>
     * @param peersRef
     * @return
     */
    private HashMap<Peer, /* @Nullable */ Peer> getCurrentPeers(List<Peer> peersRef)
    {
        HashMap<Peer, Peer> ret = new HashMap<>();
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.NODES_MAP))
        {
            for (Peer peer : peersRef)
            {
                @Nullable Peer nodesCurrentPeer;
                Node node = peer.getNode();
                if (!node.isDeleted())
                {
                    nodesCurrentPeer = node.getPeer(sysCtx);
                }
                else
                {
                    nodesCurrentPeer = null;
                }
                ret.put(peer, nodesCurrentPeer);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return ret;
    }
}
