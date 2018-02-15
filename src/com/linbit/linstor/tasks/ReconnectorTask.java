package com.linbit.linstor.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

import com.linbit.linstor.core.CtrlAuthenticationApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.tasks.TaskScheduleService.Task;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ReconnectorTask implements Task
{
    private static final int RECONNECT_SLEEP = 10_000;

    private final Object syncObj = new Object();
    private final LinkedList<Peer> peerList = new LinkedList<>();
    private final ErrorReporter errorReporter;
    private PingTask pingTask;
    private CtrlAuthenticationApiCallHandler authApiCallHandler;

    @Inject
    public ReconnectorTask(
        ErrorReporter errorReporterRef,
        CtrlAuthenticationApiCallHandler authApiCallHandlerRef
    )
    {
        errorReporter = errorReporterRef;
        authApiCallHandler = authApiCallHandlerRef;
    }

    void setPingTask(PingTask pingTaskRef)
    {
        pingTask = pingTaskRef;
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
                // no locks needed
                authApiCallHandler.completeAuthentication(peer);
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
        for (int idx = 0; idx < localList.size(); ++idx)
        {
            final Peer peer = localList.get(idx);
            if (peer.isConnected(false))
            {
                errorReporter.logTrace(
                    "Peer " + peer.getId() + " has connected. Removed from reconnectList, added to pingList."
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
                        peerList.add(peer.getConnector().reconnect(peer));
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
}
