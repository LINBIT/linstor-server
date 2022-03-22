package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.event.Level;

@Singleton
public class TaskScheduleService implements SystemService, Runnable
{
    public interface Task
    {
        long END_TASK = -1;
        /**
         * When a {@link Task} gets registered in {@link TaskScheduleService}, it will be
         * Immediately executed (calling {@link Task#run()}). <br>
         * <br>
         * This method gets called again after approximately X milliseconds after the previous
         * call (relative time, not absolute timestamp), where X is the return value of the previous call. <br>
         * <br>
         * Any negative return value will cancel the Task.
         */
        long run();

        /**
         * Called once the TaskScheduleService has started. Can be used to populate internal data-structures with
         * data that had first to be loaded from the Database.
         */
        default void initialize()
        {
        }
    }

    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "Task schedule service";
    private static final long DEFAULT_RETRY_DELAY = 60_000;

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
    private boolean shutdown = false;

    private Thread workerThread;

    private final TreeMap<Long, LinkedList<Task>> tasks = new TreeMap<>();
    private final LinkedList<Task> newTasks = new LinkedList<>();
    private final ErrorReporter errorReporter;

    @Inject
    public TaskScheduleService(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
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
        boolean needStart;
        synchronized (tasks)
        {
            needStart = !running;
            running = true;
            shutdown = false;

            // initialize tasks..
            for (Task task : newTasks)
            {
                task.initialize();
            }
        }
        if (needStart)
        {

            workerThread = new Thread(this, serviceInstanceName.displayValue);
            workerThread.start();
        }
    }

    @Override
    public void shutdown()
    {
        synchronized (tasks)
        {
            shutdown = true;
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
        try
        {
            while (!shutdown)
            {
                try
                {
                    long now = System.currentTimeMillis();
                    {
                        // If there are new tasks to be added,
                        // run the tasks and reschedule each task according
                        // to the delay that the task requested
                        Task execTask;
                        do
                        {
                            synchronized (tasks)
                            {
                                execTask = newTasks.pollFirst();
                            }
                            if (execTask != null)
                            {
                                execute(execTask, now);
                            }
                        }
                        while (execTask != null);
                    }

                    Long entryTime;
                    synchronized (tasks)
                    {
                        entryTime = tasks.firstKey();
                    }
                    now = System.currentTimeMillis();

                    // If the task list's target time is in the past or is now,
                    // remove the task list entry and run and reschedule all
                    // tasks from the task list
                    while (entryTime != null && entryTime <= now)
                    {
                        // The tasks list did not change since the firstEntry() call,
                        // so this will remove the same entry that firstEntry() selected,
                        // and it is probably faster than calling remove()
                        List<Task> taskListCopy;
                        synchronized (tasks)
                        {
                            LinkedList<Task> taskList = tasks.remove(entryTime);
                            taskListCopy = taskList == null ? Collections.emptyList() : new LinkedList<>(taskList);
                        }
                        for (Task execTask : taskListCopy)
                        {
                            execute(execTask, now);
                        }
                        synchronized (tasks)
                        {
                            entryTime = tasks.firstKey();
                        }

                        // Recheck the current time only if the task's entryTime
                        // is greater than the last known current time
                        if (entryTime > now)
                        {
                            now = System.currentTimeMillis();
                        }
                    }

                    // Default to a waitTime of zero if there are no task lists
                    // to suspend this thread until new tasks are added
                    long waitTime = 0;
                    if (entryTime != null)
                    {
                        // Set the waitTime to suspend this thread until the
                        // next task list's target time is reached
                        waitTime = entryTime - now;
                    }

                    // Suspend until new tasks are added or the target time of an
                    // existing task list is reached
                    synchronized (tasks)
                    {
                        if (!shutdown && newTasks.isEmpty())
                        {
                            tasks.wait(waitTime);
                        }
                    }
                }
                catch (InterruptedException ignored)
                {
                }
            }
        }
        catch (Exception exc)
        {
            errorReporter.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Unhandled exception caught in " + TaskScheduleService.class.getName(),
                    exc
                ),
                null,
                null,
                "This exception was generated in the service thread of the service '" + SERVICE_NAME + "'"
            );
        }
        finally
        {
            synchronized (tasks)
            {
                running = false;
            }
        }
    }

    private void execute(Task task, long now)
    {
        long delay = DEFAULT_RETRY_DELAY;
        try
        {
            delay = task.run();
        }
        catch (Exception exc)
        {
            errorReporter.reportError(
                Level.ERROR,
                new ImplementationError(
                    "Unhandled exception caught in " + TaskScheduleService.class.getName(),
                    exc
                ),
                null,
                null,
                "This exception was generated in the service thread of the service '" + SERVICE_NAME + "'"
            );
        }

        // Reschedule the task if a non-negative delay was requested
        if (delay >= 0)
        {
            long targetTime = delay + now;
            if (targetTime < 0)
            {
                targetTime = Long.MAX_VALUE;
            }
            // If a task list exists for the calculated target time,
            // add the task to the existing task list; otherwise, register
            // a new task list for the calculated target time and
            // add the task to the newly registered task list
            LinkedList<Task> taskList = tasks.get(targetTime);
            if (taskList == null)
            {
                taskList = new LinkedList<>();
                tasks.put(targetTime, taskList);
            }
            taskList.add(task);
        }
    }

    /**
     * Reschedules the given task regardless when it would have been scheduled normally.
     * A negative newDelay will cancel the task completely.
     * The task will *NOT* be executed when this method is called, especially not in the caller thread of this method.
     * Even with newDelay = 0 the task is rescheduled in the internal map, which means that the TaskScheduler's internal
     * thread will be notified to execute the task (if necessary)
     *
     * @param task
     * @param newDelay
     */
    public void rescheduleAt(Task task, long newDelay)
    {
        synchronized (tasks)
        {
            Long deleteEntry = null;
            for (Entry<Long, LinkedList<Task>> entry : tasks.entrySet())
            {
                if (entry.getValue().remove(task) && entry.getValue().isEmpty())
                {
                    deleteEntry = entry.getKey();
                }
            }
            if (deleteEntry != null)
            {
                tasks.remove(deleteEntry);
            }

            if (newDelay >= 0)
            {
                long targetTime = newDelay + System.currentTimeMillis();
                LinkedList<Task> taskList = tasks.get(targetTime);
                if (taskList == null)
                {
                    taskList = new LinkedList<>();
                    tasks.put(targetTime, taskList);
                }
                taskList.add(task);
                tasks.notify();
            }
        }
    }
}
