package com.linbit.linstor.tasks;

import java.io.IOException;
import java.util.LinkedList;

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

    public PingTask(Controller controller, ReconnectorTask reconnector)
    {
        this.controller = controller;
        this.reconnector = reconnector;

        reconnector.setPingTask(this);
    }

    public void add(Peer peer)
    {
        peerList.add(peer);
    }

    @Override
    public long run()
    {
        for (int idx = 0; idx < peerList.size(); ++idx)
        {
            final Peer peer = peerList.get(idx);

            final long lastPingReceived = peer.getLastPongReceived();
            final long lastPingSent = peer.getLastPingSent();
            if (lastPingReceived + PING_TIMEOUT < lastPingSent)
            {
                controller.getErrorReporter().logTrace(
                    "Connection to peer " + peer.getId() + " lost. Removed from pingList, added to reconnectList."
                );
                peerList.remove(peer);
                --idx;
                reconnector.add(peer);
                try
                {
                    peer.getConnector().reconnect(peer);
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    controller.getErrorReporter().reportError(ioExc);
                }
            }
            else
            {
                peer.sendPing();
            }
        }
        return PING_SLEEP;
    }
}
