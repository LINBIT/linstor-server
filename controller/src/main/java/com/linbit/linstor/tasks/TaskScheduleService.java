package com.linbit.linstor.tasks;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.SystemServiceStartException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.logging.ErrorReporter;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
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
    /**
     * <p>When adding a new task via {@link TaskScheduleService#addTask(Task)}, before the first execution of this task
     * {@link #firstRunAt()} is called. If that method returns {@link #END_TASK}, the task is canceled without being
     * executed once. If {@link #firstRunAt()} returns {@link #RUN_ASAP}, the task is started as soon as possible. </p>
     *
     * <p>When task gets added before the {@link TaskScheduleService} was started, each task's {@link #initialize()} is
     * called during the initialization phase of {@link TaskScheduleService}. The registered task is further treated
     * the same as a task that got added via {@link TaskScheduleService#addTask(Task)}.</p>
     */
    public interface Task
    {
        long END_TASK = -1;
        long RUN_ASAP = 0;

        /**
         * <p>This method gets called again approximately at the given returned timestamp (unless delayed by other
         * executed tasks) </p>
         * <p>If a tasks wants to be executed i.e. "every 10 seconds", the final return statement should include the
         * parameter scheduledAt (i.e. <code> return scheduledAt + 10_000; </code>) to prevent small but additive
         * delays caused by other tasks execution time or waiting-inaccuracies</p>
         *
         * @param scheduledAt The timestamp (absolute, in millisecond) when the current execution should have been run,
         *     but might have been delayed through the execution of previous tasks. In other words, even at the very
         *     beginning of the call, scheduledAt can largely differ (even seconds or more) from
         *     {@link System#currentTimeMillis()}
         *
         * @return The absolute timestamp in milliseconds this method wants to be called next, or {@link #END_TASK} (or
         * any other negative number) to cancel this task completely.
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
         * Called before the task is executed the first time to give the task a chance to not run whenever it gets
         * registered (or right after startup), but start with a delay.
         */
        default long firstRunAt()
        {
            return RUN_ASAP;
        }

        /**
         * Calculates the next scheduled timestamp pretending perfect previous scheduled timestamps in order to prevent
         * future executions to get delayed additively. Example:
         * If scheduleAt is 12, rescheduleInRelative is 10 and current timestamp is 41, the returned value would be 42
         * as it is the next higher number that is X * rescheduledInRelative later than scheduleAt.
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

    private final TreeMap<Long, List<Task>> tasks = new TreeMap<>();
    private final List<Task> newTasks = new ArrayList<>();
    /**
     * Tasks that are currently inside their {@link Task#run(long)} method. While a task is running it is not part of
     * {@link #tasks}, so a {@link #cancel(Task)} would not find anything to remove. Such tasks are remembered in
     * {@link #canceledWhileRunning} instead.
     */
    private final Set<Task> runningTasks = new HashSet<>();
    private final Set<Task> canceledWhileRunning = new HashSet<>();
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
        tasksLock.lock();
        try
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
        tasksLock.lock();
        try
        {
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
        tasksLock.lock();
        try
        {
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
                        final List<Task> execTaskList = new ArrayList<>(newTasks);
                        newTasks.clear();
                        if (!execTaskList.isEmpty())
                        {
                            long now = System.currentTimeMillis();
                            tasksLock.unlock();
                            for (Task execTask : execTaskList)
                            {
                                long firstRunAt = getFirstRunAt(execTask);
                                if (firstRunAt >= Task.RUN_ASAP)
                                {
                                    long scheduledAt = firstRunAt == Task.RUN_ASAP ? now : firstRunAt;
                                    if (firstRunAt <= now)
                                    {
                                        execute(execTask, scheduledAt);
                                    }
                                    else
                                    {
                                        rescheduleAt(execTask, scheduledAt);
                                    }
                                }
                            }
                            tasksLock.lock();
                        }
                    }

                    // Handle existing tasks
                    long waitTime;
                    if (!tasks.isEmpty())
                    {
                        @Nullable Long entryTime = tasks.firstKey();
                        long now = System.currentTimeMillis();

                        while (entryTime != null && entryTime <= now)
                        {
                            // Remove the task
                            Entry<Long, List<Task>> taskEntry = tasks.pollFirstEntry();
                            final List<Task> execTaskList = new ArrayList<>(taskEntry.getValue());

                            tasksLock.unlock();
                            for (Task execTask : execTaskList)
                            {
                                execute(execTask, entryTime);
                            }
                            tasksLock.lock();

                            entryTime = tasks.isEmpty() ? null : tasks.firstKey();
                            if (entryTime != null && entryTime > now)
                            {
                                now = System.currentTimeMillis();
                            }
                        }

                        // Set the waitTime to suspend this thread until the
                        // next task list's target time is reached, or if there
                        // are no more tasks, wait for DEFAULT_RETRY_DELAY
                        waitTime = entryTime != null ? entryTime - now : DEFAULT_RETRY_DELAY;
                    }
                    else
                    {
                        waitTime = DEFAULT_RETRY_DELAY;
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
                "This exception was generated in the service thread of the service '" + SERVICE_NAME + "'"
            );
        }
        finally
        {
            running = false;
            tasksLock.unlock();
        }
    }

    private long getFirstRunAt(Task execTask)
    {
        long ret;
        try
        {
            ret = execTask.firstRunAt();
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
                "This exception was generated in the service thread of the service '" + SERVICE_NAME + "' " +
                    "during firstRunAt check of task '" + execTask.getClass().getSimpleName() + "', '" + execTask + "'."
            );
            ret = Task.RUN_ASAP;
        }
        return ret;
    }

    private void execute(Task task, long scheduledAt)
    {
        tasksLock.lock();
        try
        {
            runningTasks.add(task);
        }
        finally
        {
            tasksLock.unlock();
        }

        long rescheduleAt = scheduledAt + DEFAULT_RETRY_DELAY;
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, ErrorReporter.getNewLogId()))
        {
            rescheduleAt = task.run(scheduledAt);
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
                "This exception was generated in the service thread of the service '" + SERVICE_NAME + "'"
            );
        }

        tasksLock.lock();
        try
        {
            runningTasks.remove(task);
            boolean canceled = canceledWhileRunning.remove(task);
            // Reschedule the task if a non-negative timestamp was requested, unless the task was canceled while it
            // was running
            if (!canceled && rescheduleAt >= 0)
            {
                rescheduleAt(task, rescheduleAt);
            }
        }
        finally
        {
            tasksLock.unlock();
        }
    }

    /**
     * <p>Reschedules the given task at the given timestamp ({@code absoluteTimestampInMs}). Unlike
     * {@link #rescheduleIn(Task, long)}, this method's {@code absoluteTimestampInMs} is an absolute timestamp.</p>
     *
     * <p>Makes sure the given task gets removed from all scheduled tasks and only (re-) inserted with the given
     * {@code absoluteTimestampInMs}-timestamp, effectively deduplicates the given task.</p>
     * <p>The task is only inserted in the internal map if the given {@code absoluteTimestampInMs} parameter is
     * {@code >= 0}. A negative value cancels the task, see {@link #cancel(Task)}.</p>
     */
    public void rescheduleAt(Task task, long absoluteTimestampInMs)
    {
        tasksLock.lock();
        try
        {
            List<Long> entriesToDelete = new ArrayList<>();
            for (Entry<Long, List<Task>> entry : tasks.entrySet())
            {
                if (entry.getValue().remove(task) && entry.getValue().isEmpty())
                {
                    entriesToDelete.add(entry.getKey());
                }
            }
            for (Long entryToDelete : entriesToDelete)
            {
                tasks.remove(entryToDelete);
            }

            if (absoluteTimestampInMs >= Task.RUN_ASAP)
            {
                // a reschedule after a cancel reactivates the task, even if it is still running
                canceledWhileRunning.remove(task);
                List<Task> taskList = tasks.get(absoluteTimestampInMs);
                if (taskList == null)
                {
                    taskList = new ArrayList<>();
                    tasks.put(absoluteTimestampInMs, taskList);
                }
                taskList.add(task);
                tasksCond.signal();
            }
            else if (runningTasks.contains(task))
            {
                // the task is not in the map right now, so remember to ignore the timestamp its run() returns
                canceledWhileRunning.add(task);
            }
        }
        finally
        {
            tasksLock.unlock();
        }
    }

    /**
     * <p>Reschedules the given task with the given <b>relative</b> delay to the current time.</p>
     * <p>The usage is intended as something like <code>rescheduleIn(myTask, 10_000);</code> to reschedule
     * {@code myTask}
     * in 10s from now.</p>
     *
     * <p>A negative newDelay will cancel the task completely.</p>
     *
     * <p>
     * The task will *NOT* be executed when this method is called, especially not in the caller thread of this method.
     * Even with newDelay = 0 the task is rescheduled in the internal map, which means that the TaskScheduler's internal
     * thread will be notified to execute the task (if necessary)</p>
     */
    public void rescheduleIn(Task task, long relativeDelayInMs)
    {
        rescheduleAt(task, relativeDelayInMs < 0 ? relativeDelayInMs : System.currentTimeMillis() + relativeDelayInMs);
    }

    /**
     * <p>Cancels the given {@code Task}.</p>
     *
     * <p>If the task is currently being executed, it will <b>not</b> be interrupted. Instead, the timestamp returned
     * by its {@link Task#run(long)} is ignored so that the task is not rescheduled. A {@link #rescheduleAt(Task, long)}
     * or {@link #rescheduleIn(Task, long)} with a non-negative value after the cancel reactivates the task.</p>
     */
    public void cancel(Task task)
    {
        rescheduleAt(task, Task.END_TASK);
    }
}
