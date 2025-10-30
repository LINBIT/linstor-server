package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.MDC;
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
         * This method gets called again approximately at the given returned timestamp (unless delayed by other executed
         * tasks) <br>
         * If a tasks wants to be executed i.e. "every 10 seconds", the final return statement should include the
         * parameter scheduledAt (i.e. <code> return scheduledAt + 10_000; </code>) to prevent small but additive
         * delays caused by other tasks execution time or waiting-inaccuracies
         * <br>
         * Any negative return value will prevent the task from being rescheduled.
         *
         * @param scheduledAt The timestamp when the current execution should have been run, but might have been delayed
         *     through the execution of previous tasks. In other words, even at the very beginning of the call,
         *     scheduledAt can largely differ (even seconds or more) from {@link System#currentTimeMillis()}
         */
        long run(long scheduledAt);

        /**
         * Called once the TaskScheduleService has started. Can be used to populate internal data-structures with
         * data that had first to be loaded from the Database.
         */
        default void initialize()
        {
        }

        /**
         * Calculates the next scheduled timestamp pretending perfect previous scheduled timestamps in order to prevent
         * future executions to get delayed additively. Example:
         * If scheduleAt is 12, rescheduleInRelative is 10 and current timestamp is 41, the returned value would be 42
         * as it is the next higher number that is X * rescheduledInRelative later than scheduleAt.
         *
         * @param scheduledAt
         * @param rescheduleInRelative
         *
         * @return
         */
        default long getNextFutureReschedule(long scheduledAt, long rescheduleInRelative)
        {
            long now = System.currentTimeMillis();
            // in order to prevent using Math.ceil and casting the result to long:
            long ceil;
            if (now == scheduledAt)
            {
                ceil = 1;
            }
            else
            {
                ceil = (now - scheduledAt + (rescheduleInRelative - 1)) / rescheduleInRelative;
            }
            return ceil * rescheduleInRelative + scheduledAt;
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

    private final Lock tasksLock;
    private final Condition tasksCond;

    private @Nullable Thread workerThread;

    private final TreeMap<Long, LinkedList<Task>> tasks = new TreeMap<>();
    private final LinkedList<Task> newTasks = new LinkedList<>();
    private final ErrorReporter errorReporter;

    @Inject
    public TaskScheduleService(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
        serviceInstanceName = SERVICE_NAME;
        tasksLock = new ReentrantLock();
        tasksCond = tasksLock.newCondition();
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
        try
        {
            tasksLock.lock();
            needStart = !running;
            running = true;
            shutdown = false;

            // initialize tasks..
            for (Task task : newTasks)
            {
                task.initialize();
            }
        }
        finally
        {
            tasksLock.unlock();
        }
        if (needStart)
        {

            workerThread = new Thread(this, serviceInstanceName.displayValue);
            workerThread.start();
        }
    }

    @Override
    public void shutdown(boolean ignoredJvmShutdownRef)
    {
        try
        {
            tasksLock.lock();
            shutdown = true;
            tasksCond.signal();
        }
        finally
        {
            tasksLock.unlock();
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
        try
        {
            tasksLock.lock();
            newTasks.add(task);
            tasksCond.signal();
        }
        finally
        {
            tasksLock.unlock();
        }
    }

    @Override
    public void run()
    {
        try
        {
            tasksLock.lock();
            while (!shutdown)
            {
                try
                {
                    // Handle new tasks
                    {
                        // Run any new tasks and reschedule each task according to
                        // the delay that the task requested
                        final List<Task> execTaskList = new LinkedList<>(newTasks);
                        newTasks.clear();
                        if (!execTaskList.isEmpty())
                        {
                            long now = System.currentTimeMillis();
                            tasksLock.unlock();
                            for (Task execTask : execTaskList)
                            {
                                execute(execTask, now);
                            }
                            tasksLock.lock();
                        }
                    }

                    // Handle existing tasks
                    long waitTime;
                    Long entryTime = tasks.firstKey();
                    if (entryTime != null)
                    {
                        long now = System.currentTimeMillis();

                        while (entryTime != null && entryTime <= now)
                        {
                            // Remove the task
                            Entry<Long, LinkedList<Task>> taskEntry = tasks.pollFirstEntry();
                            final List<Task> execTaskList = new LinkedList<>(taskEntry.getValue());

                            tasksLock.unlock();
                            for (Task execTask : execTaskList)
                            {
                                execute(execTask, entryTime);
                            }
                            tasksLock.lock();

                            entryTime = tasks.firstKey();
                            if (entryTime != null && entryTime > now)
                            {
                                now = System.currentTimeMillis();
                            }
                        }

                        // Set the waitTime to suspend this thread until the
                        // next task list's target time is reached, or if there
                        // are no more tasks, wait until a wakeup event occurs (0)
                        waitTime = entryTime != null ? entryTime - now : 0;
                    }
                    else
                    {
                        waitTime = 0;
                    }

                    if (!shutdown && newTasks.isEmpty())
                    {
                        // Suspend until new tasks are added or the target time of an
                        // existing task list is reached
                        tasksCond.await(waitTime, TimeUnit.MILLISECONDS);
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
            running = false;
            tasksLock.unlock();
        }
    }

    private void execute(Task task, long scheduledAt)
    {
        long delay = scheduledAt + DEFAULT_RETRY_DELAY;
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            delay = task.run(scheduledAt);
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
            // If a task list exists for the calculated target time,
            // add the task to the existing task list; otherwise, register
            // a new task list for the calculated target time and
            // add the task to the newly registered task list
            try
            {
                tasksLock.lock();
                LinkedList<Task> taskList = tasks.get(delay);
                if (taskList == null)
                {
                    taskList = new LinkedList<>();
                    tasks.put(delay, taskList);
                }
                taskList.add(task);
            }
            finally
            {
                tasksLock.unlock();
            }
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
        try
        {
            tasksLock.lock();
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
                tasksCond.signal();
            }
        }
        finally
        {
            tasksLock.unlock();
        }
    }
}
