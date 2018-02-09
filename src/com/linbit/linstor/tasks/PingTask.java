package com.linbit.linstor.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

public class PingTask implements Task
{
    private static final int PING_TIMEOUT = 5_000;
    private static final long PING_SLEEP = 1_000;

    private final LinkedList<Peer> peerList = new LinkedList<>();
    private final Controller controller;
    private final ReconnectorTask reconnector;

    public PingTask(Controller controllerRef, ReconnectorTask reconnectorRef)
    {
        controller = controllerRef;
        reconnector = reconnectorRef;

        reconnector.setPingTask(this);
    }

    public void add(Peer peer)
    {
        peerList.add(peer);
    }

    @Override
    public long run()
    {
        final List<Peer> peersToRemove = new ArrayList<>();

        for (final Peer peer : peerList)
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
                    reconnect = true;
                }
            }
            if (reconnect)
            {
                controller.getErrorReporter().logTrace(
                    "Connection to peer " + peer.getId() + " lost. Removed from pingList, added to reconnectList."
                );
                peersToRemove.add(peer);
                try
                {
                    reconnector.add(peer.getConnector().reconnect(peer));
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    controller.getErrorReporter().reportError(ioExc);
                }
            }
        }
        peerList.removeAll(peersToRemove);
        return PING_SLEEP;
    }
}
