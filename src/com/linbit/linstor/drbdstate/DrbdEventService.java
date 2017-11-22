package com.linbit.linstor.drbdstate;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.extproc.DaemonHandler;
import com.linbit.extproc.OutputProxy.Event;
import com.linbit.extproc.OutputProxy.ExceptionEvent;
import com.linbit.extproc.OutputProxy.StdErrEvent;
import com.linbit.extproc.OutputProxy.StdOutEvent;

public class DrbdEventService implements SystemService, Runnable
{
    public static final ServiceName SERVICE_NAME;
    public static final String INSTANCE_PREFIX = "DrbdEventService-";
    public static final String SERVICE_INFO = "DrbdEventService";
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger(0);

    private ServiceName instanceName;
    private boolean started = false;

    private final BlockingDeque<Event> eventDeque;
    private final Thread thread;
    private boolean running;
    private boolean startable;
    private boolean waitingForRestart;

    private final EventsTracker eventsTracker;
    private boolean needsReinitialize = false;

    private DaemonHandler demonHandler;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("DrbdEventService");
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    public DrbdEventService(final StateTracker trackerRef)
    {
        try
        {
            instanceName = new ServiceName(INSTANCE_PREFIX + INSTANCE_COUNT.incrementAndGet());
            eventDeque = new LinkedBlockingDeque<>();
            thread = new Thread(this, "DrbdEventService");
            demonHandler = new DaemonHandler(eventDeque, "drbdsetup", "events2", "all");
            startable = true;
            running = false;
            eventsTracker = new EventsTracker(trackerRef);
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new ImplementationError(invalidNameExc);
        }
    }

    @Override
    public void run()
    {
        while (startable)
        {
            while (running)
            {
                Event event;
                try
                {
                    event = eventDeque.take();
                    if (event instanceof StdOutEvent)
                    {
                        eventsTracker.receiveEvent(new String(((StdOutEvent) event).data));
                    }
                    else
                    if (event instanceof StdErrEvent)
                    {
                        demonHandler.stop(true);
                        demonHandler.start();
                    }
                    else
                    if (event instanceof ExceptionEvent)
                    {
                        // FIXME: Report the exception to the controller
                    }
                    else
                    if (event instanceof PoisonEvent)
                    {
                        break;
                    }
                }
                catch (InterruptedException | IOException exc)
                {
                    if (running)
                    {
                        // FIXME: Error reporting required
                        exc.printStackTrace();
                    }
                }
                catch (EventsSourceException exc)
                {
                    // FIXME: Error reporting required
                    exc.printStackTrace();
                }
            }
            synchronized (this)
            {
                if (startable && !running)
                {
                    try
                    {
                        waitingForRestart = true;
                        notifyAll();
                        wait();
                        waitingForRestart = false;
                    }
                    catch (InterruptedException intrExc)
                    {
                        // FIXME: Error reporting required
                        intrExc.printStackTrace();
                    }
                }
            }
        }
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
        return instanceName;
    }

    @Override
    public boolean isStarted()
    {
        return started;
    }

    @Override
    public void setServiceInstanceName(ServiceName instanceName)
    {
        this.instanceName = instanceName;
    }

    @Override
    public void start()
    {
        if (needsReinitialize)
        {
            eventsTracker.reinitializing();
        }
        needsReinitialize = true;
        running = true;
        thread.start();
        try
        {
            demonHandler.start();
        }
        catch (IOException exc)
        {
            // FIXME: Error reporting required
            exc.printStackTrace();
        }
        synchronized (this)
        {
            notifyAll();
        }
        started = true;
    }

    @Override
    public void shutdown()
    {
        running = false;
        demonHandler.stop(true);
        thread.interrupt();
        eventDeque.addFirst(new PoisonEvent());
        started = false;
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        synchronized (this)
        {
            if (!waitingForRestart)
            {
                wait(timeout);
            }
        }
    }

    private static class PoisonEvent implements Event
    {
    }
}
