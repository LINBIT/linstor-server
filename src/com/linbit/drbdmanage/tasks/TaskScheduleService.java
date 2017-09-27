package com.linbit.drbdmanage.tasks;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.event.Level;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.drbdmanage.DrbdManageException;
import com.linbit.drbdmanage.DrbdManageRuntimeException;
import com.linbit.drbdmanage.core.Controller;

public class TaskScheduleService implements SystemService, Runnable
{
    public static interface Task
    {
        /**
         * When a {@link Task} gets registered in {@link TaskScheduleService}, it will be
         * Immediately executed (calling {@link Task#run()}). <br />
         * <br />
         * This method gets called again after approximately X milliseconds after the previous
         * call (relative time, not absolute timestamp), where X is the return value of the previous call. <br />
         * <br />
         * Any negative return value will cancel the Task.
         */
        public long run();
    }

    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "Task schedule service";

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("TaskScheduleService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class contains an invalid name constant",
                    TaskScheduleService.class.getName()
                ),
                nameExc
            );
        }
    }

    private ServiceName serviceInstanceName;
    private boolean running = false;

    private Thread workerThread;

    private final TreeMap<Long, Task> tasks = new TreeMap<>();
    private final LinkedList<Task> newTasks = new LinkedList<>();
    private final Controller controller;

    public TaskScheduleService(Controller controller)
    {
        this.controller = controller;
        serviceInstanceName = SERVICE_NAME;
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
        if (workerThread != null && workerThread.isAlive())
        {
            shutdown();
            try
            {
                awaitShutdown(1000);
            }
            catch (InterruptedException interruptedExc)
            {
                throw new DrbdManageRuntimeException(
                    "Waiting for stop of old TaskScheduleService thread was interrupted",
                    interruptedExc
                );
            }
            if (workerThread.isAlive())
            {
                throw new DrbdManageRuntimeException("Could not kill already running TaskScheduleService-Thread");
            }
        }
        workerThread = new Thread(this, serviceInstanceName.displayValue);
        workerThread.start();
        running = true;
    }

    @Override
    public void shutdown()
    {
        synchronized (tasks)
        {
            running = false;
            tasks.notify();
        }
    }

    @Override
    public void awaitShutdown(long timeout) throws InterruptedException
    {
        if (workerThread != null)
        {
            workerThread.join(timeout);
        }
    }

    public void addTask(Task task)
    {
        synchronized (tasks)
        {
            newTasks.add(task);
            tasks.notify();
        }
    }

    @Override
    public void run()
    {
        while (running)
        {
            try
            {
                while (!newTasks.isEmpty())
                {
                    execute(newTasks.removeFirst());
                }

                if (tasks.isEmpty())
                {
                    synchronized (tasks)
                    {
                        if (newTasks.isEmpty())
                        {
                            tasks.wait();
                        }
                    }
                }
                else
                {
                    Entry<Long, Task> nextEntry = tasks.firstEntry();
                    while (nextEntry.getKey() < System.currentTimeMillis())
                    {
                        tasks.remove(nextEntry.getKey());

                        execute(nextEntry.getValue());

                        nextEntry = tasks.firstEntry();
                    }

                    Long sleepUntilTimestamp = nextEntry.getKey();
                    long now = System.currentTimeMillis();
                    long sleep = sleepUntilTimestamp - now;
                    if (sleep > 0)
                    {
                        synchronized (tasks)
                        {
                            tasks.wait(sleep);
                        }
                    }
                    // else: sleep == 0 is equals to tasks.wait()
                    // sleep < 0 will throw IllegalArgumentException
                }
            }
            catch (InterruptedException e)
            {
                if (running)
                {
                    controller.getErrorReporter().reportProblem(
                        Level.WARN,
                        new DrbdManageException(
                            "TaskScheduleService got interrupted unexpectedly",
                            e
                        ),
                        null,
                        null,
                        "TaskScheduleService got interrupted unexpectedly"
                    );
                }
            }
        }
    }

    private void execute(Task task)
    {
        long reRun = task.run() + System.currentTimeMillis();

        long tmp = 0;
        boolean added = false;
        while (!added)
        {
            if (!tasks.containsKey(reRun + tmp))
            {
                tasks.put(reRun + tmp, task);
                added = true;
            }
            tmp++;
        }
    }
}
