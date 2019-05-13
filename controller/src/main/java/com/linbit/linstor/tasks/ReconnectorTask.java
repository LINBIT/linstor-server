package com.linbit.linstor.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import com.linbit.linstor.Node;
import com.linbit.linstor.core.CtrlAuthenticator;
import com.linbit.linstor.core.SatelliteConnector;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

@Singleton
public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;

    private final Object syncObj = new Object();
    private final LinkedList<Peer> peerList = new LinkedList<>();
    private final ErrorReporter errorReporter;
    private PingTask pingTask;
    private final Provider<CtrlAuthenticator> authenticatorProvider;
    private final Provider<SatelliteConnector> satelliteConnector;

    @Inject
    public ReconnectorTask(
        ErrorReporter errorReporterRef,
        Provider<CtrlAuthenticator> authenticatorRef,
        Provider<SatelliteConnector> satelliteConnectorRef
    )
    {
        errorReporter = errorReporterRef;
        authenticatorProvider = authenticatorRef;
        satelliteConnector = satelliteConnectorRef;
    }

    void setPingTask(PingTask pingTaskRef)
    {
        pingTask = pingTaskRef;
    }

    public void add(Peer peer, boolean authenticateImmediately)
    {
        synchronized (syncObj)
        {
            if (authenticateImmediately && peer.isConnected(false))
            {
                // no locks needed
                authenticatorProvider.get().sendAuthentication(peer);
                pingTask.add(peer);
            }
            else
            {
                peerList.add(peer);
            }
        }
    }

    public void peerConnected(Peer peer)
    {
        synchronized (syncObj)
        {
            if (peerList.remove(peer) && pingTask != null)
            {
                // no locks needed
                authenticatorProvider.get().sendAuthentication(peer);
                pingTask.add(peer);
            }
        }
    }

    public void removePeer(Peer peer)
    {
        synchronized (syncObj)
        {
            peerList.remove(peer);
            pingTask.remove(peer);
        }
    }

    @Override
    public long run()
    {
        ArrayList<Peer> localList;
        synchronized (syncObj)
        {
            localList = new ArrayList<>(peerList);
        }
        for (final Peer peer : localList)
        {
            if (peer.isConnected(false))
            {
                errorReporter.logTrace(
                    peer + " has connected. Removed from reconnectList, added to pingList."
                );
                peerConnected(peer);
            }
            else
            {
                errorReporter.logTrace(
                    "Peer " + peer.getId() + " has not connected yet, retrying connect."
                );
                try
                {
                    synchronized (syncObj)
                    {
                        Node node = peer.getNode();
                        if (node != null && !node.isDeleted())
                        {
                            peerList.add(peer.getConnector().reconnect(peer));
                        }
                        else
                        {
                            if (node == null)
                            {
                                errorReporter.logTrace(
                                    "Peer %s's node is null (possibly rollbacked), removing from reconnect list",
                                    peer.getId()
                                );
                            }
                            else
                            {
                                errorReporter.logTrace(
                                    "Peer %s's node got deleted, removing from reconnect list",
                                    peer.getId()
                                );
                            }
                        }
                        peerList.remove(peer);
                    }
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    errorReporter.reportError(ioExc);
                }
            }
        }
        return RECONNECT_SLEEP;
    }

    public void startReconnecting(Collection<Node> nodes, AccessContext initCtx)
    {
        /*
         * We need this method so that all nodes are added starting connecting while having
         * this syncObj. If we would give up the syncObj between two startConnecting calls
         * we might run into a deadlock where one thread tries to connect (awaits authentication)
         * and another thread tries to start connecting the next node.
         */
        synchronized (syncObj)
        {
            SatelliteConnector stltConnector = satelliteConnector.get();
            for (Node node : nodes)
            {
                errorReporter.logDebug("Reconnecting to node '" + node.getName() + "'.");
                stltConnector.startConnecting(node, initCtx);
            }
        }
    }
}
