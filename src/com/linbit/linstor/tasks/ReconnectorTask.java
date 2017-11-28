package com.linbit.linstor.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;

    private final Object syncObj = new Object();
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
        synchronized (syncObj)
        {
            peerList.add(peer);
        }
    }

    public void peerConnected(Peer peer)
    {
        synchronized (syncObj)
        {
            if (peerList.remove(peer) && pingTask != null)
            {
                controller.getApiCallHandler().completeSatelliteAuthentication(peer);
                pingTask.add(peer);
            }
        }
    }

    @Override
    public long run()
    {
        ArrayList<Peer> localList = new ArrayList<>(peerList);
        for (int idx = 0; idx < localList.size(); ++idx)
        {
            final Peer peer = localList.get(idx);
            if (peer.isConnected())
            {
                controller.getErrorReporter().logTrace(
                    "Peer " + peer.getId() + " has connected. Removed from reconnectList, added to pingList."
                );
                peerConnected(peer);
            }
            else
            {
                controller.getErrorReporter().logTrace(
                    "Peer " + peer.getId() + " has not connected yet, retrying connect."
                );
                try
                {
                    synchronized (syncObj)
                    {
                        peerList.remove(peer);
                        peerList.add(peer.getConnector().reconnect(peer));
                    }
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
