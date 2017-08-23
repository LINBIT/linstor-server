package com.linbit.drbdmanage.netcom;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.NegativeTimeException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.DrbdException;
import com.linbit.drbdmanage.Controller;
import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.timer.Action;

/**
 *
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */

//TODO try to check if a successfully established connection breaks suddenly - then start reconnecting
public class ReconnectorService implements SystemService
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "Connector -> Satellite reconnector service";

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("ReconnectorService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class contains an invalid name constant",
                    ReconnectorService.class.getName()
                ),
                nameExc
            );
        }
    }

    private final Controller controller;

    private final List<ReconnectorActionData> list;
    private final ReconnectorAction action;

    private final Thread workerThread;
    private final Runnable workerRunnable;
    private ArrayBlockingQueue<List<ReconnectorActionData>> workerQueue;

    private ServiceName serviceInstanceName;
    private boolean running = false;

    public ReconnectorService(Controller controller)
    {
        this.controller = controller;

        serviceInstanceName = SERVICE_NAME;

        list = new LinkedList<>();
        workerRunnable = new ReconnectorRunnable();
        workerQueue = new ArrayBlockingQueue<>(1);
        workerThread = new Thread(workerRunnable, serviceInstanceName.displayValue);

        action = new ReconnectorAction();
    }

    @Override
    public ServiceName getServiceName()
    {
        return SERVICE_NAME;
    }

    @Override
    public String getServiceInfo()
    {
        return SERVICE_INFO;
    }

    @Override
    public ServiceName getInstanceName()
    {
        return serviceInstanceName;
    }

    @Override
    public boolean isStarted()
    {
        return running;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName == null)
        {
            serviceInstanceName = SERVICE_NAME;
        }
        else
        {
            serviceInstanceName = instanceName;
        }
        if (workerThread != null)
        {
            workerThread.setName(serviceInstanceName.displayValue);
        }
    }

    @Override
    public void start() throws SystemServiceStartException
    {
        workerThread.start();
        running = true;
    }

    @Override
    public void shutdown()
    {
        running = false;
        workerQueue.offer(new ArrayList<ReconnectorActionData>());
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        workerThread.join(timeout);
    }

    public void addReconnect(
        InetSocketAddress satelliteAddress,
        TcpConnector tcpConnector,
        Peer pendingPeer
    )
    {
        list.add(new ReconnectorActionData(satelliteAddress, tcpConnector, pendingPeer));
        ensureQueued();
    }

    private void ensureQueued() throws ImplementationError
    {
        synchronized (action)
        {
            if (!action.isQueued())
            {
                try
                {
                    controller.getTimer().addDelayedAction(10_000L, action);
                    action.setQueued(true);
                }
                catch (NegativeTimeException negativeTimeExc)
                {
                    throw new ImplementationError(
                        "Hardcoded ReconnectAction-delay threw a negative time exc...",
                        negativeTimeExc
                    );
                }
                catch (ValueOutOfRangeException valOORangeExc)
                {
                    throw new ImplementationError(
                        "Hardcoded ReconnectAction-delay threw a value out of range exc...",
                        valOORangeExc
                    );
                }
            }
        }
    }

    private class ReconnectorActionData
    {
        private final InetSocketAddress satelliteAddress;
        private final TcpConnector tcpConnector;
        private Peer peer;

        public ReconnectorActionData(
            InetSocketAddress satelliteAddress,
            TcpConnector tcpConnector,
            Peer peer
        )
        {
            this.satelliteAddress = satelliteAddress;
            this.tcpConnector = tcpConnector;
            this.peer = peer;
        }
    }

    private class ReconnectorAction implements Action<String>
    {
        private boolean queued = false;

        @Override
        public void run()
        {
            synchronized (this)
            {
                if (!workerQueue.offer(list))
                {
                    controller.getErrorReporter().reportError(
                        new DrbdException(
                            "Reconnector too slow",
                            "ReconnectorWorker did not finished before new reconnect-actions were added",
                            String.format(
                                "Maybe they are too many reconnect-actions pending (currently %s)",
                                list.size()
                            ),
                            "The main reason of this exception is that too many satellites have connection- / " +
                                "reachability-problems. Please infestigate that.",
                            null
                        )
                    );
                }
                setQueued(false);
            }
        }

        @Override
        public String getId()
        {
            return "ReconnectorAction";
        }

        public boolean isQueued()
        {
            return queued;
        }

        public void setQueued(boolean queued)
        {
            this.queued = queued;
        }
    }

    private class ReconnectorRunnable implements Runnable
    {
        @Override
        public void run()
        {
            while (running)
            {
                try
                {
                    List<ReconnectorActionData> workingList = workerQueue.take();
                    for (int idx = 0; idx < workingList.size(); ++idx)
                    {
                        ReconnectorActionData data = workingList.get(idx);
                        if (data.peer.isConnected())
                        {
                            controller.getErrorReporter().logInfo(
                                "peer " + data.satelliteAddress + " has connected. Removing from reconnect-watchlist");
                            workingList.remove(idx);
                            --idx;
                        }
                        else
                        {
                            controller.getErrorReporter().logInfo(
                                "peer " + data.satelliteAddress + " has not connected yet - retrying connect");
                            data.peer.closeConnection();
                            data.peer = data.tcpConnector.connect(data.satelliteAddress);
                        }
                    }
                    if (!workingList.isEmpty())
                    {
                        ensureQueued();
                    }
                }
                catch (InterruptedException e)
                {
                    if (running)
                    {
                        controller.getErrorReporter().reportError(
                            new DrbdManageException(
                                "Interrupted exception catched"
                                // TODO: detailed error reporting
                            )
                        );
                    }
                }
                catch (IOException ioExc)
                {
                    // TODO: detailed error reporting
                    controller.getErrorReporter().reportError(ioExc);
                }
            }
        }
    }
}
