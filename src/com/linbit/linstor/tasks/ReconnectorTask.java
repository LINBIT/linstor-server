package com.linbit.linstor.tasks;

import java.io.IOException;
import java.util.LinkedList;

import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;

    private final LinkedList<Peer> peerList = new LinkedList<>();
    private final Controller controller;
    private PingTask pingTask;

    public ReconnectorTask(Controller controller)
    {
        this.controller = controller;
    }

    void setPingTask(PingTask pingTask)
    {
        this.pingTask = pingTask;
    }

    public void add(Peer peer)
    {
        peerList.add(peer);
    }

    public void peerConnected(Peer peer)
    {
        peerList.remove(peer);
        pingTask.add(peer);
    }

    @Override
    public long run()
    {
        for (int idx = 0; idx < peerList.size(); ++idx)
        {
            final Peer peer = peerList.get(idx);
            if (peer.isConnected())
            {
                controller.getErrorReporter().logInfo(
                    "peer " + peer.getId() + " has connected. -reconnectList, +pingList");
                peerList.remove(idx);
                --idx;
                if (pingTask != null)
                {
                    pingTask.add(peer);
                }
            }
            else
            {
                controller.getErrorReporter().logInfo(
                    "peer " + peer.getId() + " has not connected yet - retrying connect");
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
        }
        return RECONNECT_SLEEP;
    }
}
