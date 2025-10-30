package com.linbit.timer;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.NegativeTimeException;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;

import java.util.Map;
import java.util.TreeMap;

/**
 * Implements a timer that runs Actions at a predefined point in time
 * or after a specified delay
 *
 * @param <K> Type of the Action's unique identifier
 * @param <V> Type (e.g., subclass) of Action instances used by this timer
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class GenericTimer<K extends Comparable<K>, V extends Action<K>>
    implements Timer<K, V>, SystemService
{
    private static final ServiceName SERVICE_NAME;
    private static final String SERVICE_INFO = "Timed actions scheduler";

    // Maps interrupt time value to action id & action object
    private final TreeMap<Long, TreeMap<K, V>> timerMap;

    // Maps action id to interrupt time value
    private final TreeMap<K, Long> actionMap;

    private boolean stopFlag = false;

    private long schedWakeupTime = 0;

    private @Nullable ActionScheduler<K, V> sched;

    private ServiceName serviceInstanceName;

    static
    {
        try
        {
            SERVICE_NAME = new ServiceName("TimerEventService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                String.format(
                    "%s class contains an invalid name constant",
                    GenericTimer.class.getName()
                ),
                nameExc
            );
        }
    }

    /**
     * Constructs a new timer instance
     */
    public GenericTimer()
    {
        timerMap = new TreeMap<>();
        actionMap = new TreeMap<>();
        sched = null;
        serviceInstanceName = SERVICE_NAME;
    }

    /**
     * Adds an action to perform after a delay
     *
     * Note that actions are performed asynchronously and that a newly added action may
     * be performed before this method returns.
     *
     * Actions with a delay of zero are performed immediately in the context of the thread that called
     * the addDelayedAction() method.
     *
     * An action holds this timer's lock while it is being called.
     *
     * While being performed, an action can add or cancel other actions.
     * It can not re-add itself.
     * It could theoretically also cancel itself, although this serves no purpose,
     * because actions are automatically cancel upon completion.
     *
     * @param delay Delay in milliseconds
     * @param actionObj Action to perform
     * @throws NegativeTimeException If delay is a negative value
     * @throws ValueOutOfRangeException If the calculated target time (delay and current time)
     *     would overflow Long.MAX_VALUE
     */
    @Override
    public void addDelayedAction(Long delay, V actionObj)
        throws NegativeTimeException, ValueOutOfRangeException
    {
        if (delay < 0)
        {
            throw new NegativeTimeException();
        }
        else
        if (delay == 0)
        {
            // Action without a delay triggers immediately
            // To be consistent with scheduled execution, take this timer's lock
            synchronized (this)
            {
                actionObj.run();
            }
        }
        else
        {
            long currentTime = System.currentTimeMillis();
            try
            {
                Long wakeupTime = Math.addExact(currentTime, delay);
                addScheduledAction(wakeupTime, actionObj);
            }
            catch (ArithmeticException arithExc)
            {
                throw new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_HIGH);
            }
        }
    }

    /**
     * Adds an action to perform at a specified time
     *
     * Note that actions are performed asynchronously and that a newly added action may
     * be performed before this method returns.
     *
     * An action holds this timer's lock while it is being called.
     *
     * While being performed, an action can add or cancel other actions.
     * It can not re-add itself.
     * It could theoretically also cancel itself, although this serves no purpose,
     * because actions are automatically cancel upon completion.
     *
     * @param scheduledTime The timestamp, in milliseconds, of the point in time
     *     where the action should be performed. This timestamp has the same time reference
     *     as System.currentTimeMillis()
     * @param actionObj Action to perform
     */
    @Override
    public void addScheduledAction(Long scheduledTime, V actionObj)
    {
        K actionId = actionObj.getId();
        synchronized (this)
        {
            if (!actionMap.containsKey(actionId))
            {
                TreeMap<K, V> timeSlotMap = timerMap.get(scheduledTime);
                if (timeSlotMap == null)
                {
                    timeSlotMap = new TreeMap<>();
                    timerMap.put(scheduledTime, timeSlotMap);
                }
                timeSlotMap.put(actionId, actionObj);
                actionMap.put(actionId, scheduledTime);

                if (schedWakeupTime == 0 || scheduledTime < schedWakeupTime)
                {
                    notify();
                }
            }
        }
    }

    /**
     * Cancels an action
     *
     * Canceled actions are guaranteed not to be performed anymore after returning from this method.
     *
     * @param actionId Action to cancel
     */
    @Override
    public void cancelAction(K actionId)
    {
        synchronized (this)
        {
            Long wakeupTime = actionMap.get(actionId);
            if (wakeupTime != null)
            {
                TreeMap<K, V> timeSlotMap = timerMap.get(wakeupTime);
                if (timeSlotMap != null)
                {
                    timeSlotMap.remove(actionId);
                    if (timeSlotMap.isEmpty())
                    {
                        timerMap.remove(wakeupTime);
                        if (wakeupTime == schedWakeupTime)
                        {
                            notify();
                        }
                    }
                }
                actionMap.remove(actionId);
            }
        }
    }

    /**
     * Starts this timer instance's ActionScheduler thread
     */
    @Override
    public void start()
    {
        synchronized (this)
        {
            if (sched == null)
            {
                stopFlag = false;
                sched = new ActionScheduler<>(this);
                sched.setName(serviceInstanceName.getDisplayName());
                sched.start();
            }
        }
    }

    /**
     * Shuts down this timer instance's ActionScheduler thread
     *
     * This method does not wait for the ActionScheduler thread to end.
     */
    @Override
    public void shutdown(boolean ignoredJvmShutdownRef)
    {
        synchronized (this)
        {
            stopFlag = true;
            notify();
        }
    }

    @Override
    public void awaitShutdown(long timeout)
        throws InterruptedException
    {
        @Nullable Thread joinThr = null;
        synchronized (this)
        {
            joinThr = sched;
        }
        if (joinThr != null)
        {
            joinThr.join(timeout);
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
        return serviceInstanceName;
    }

    @Override
    public synchronized boolean isStarted()
    {
        return sched != null;
    }

    /**
     * Sets the name of this timer and its action scheduler thread
     *
     * @param instanceName The name for this timer and its action scheduler thread
     */
    @Override
    public synchronized void setServiceInstanceName(ServiceName instanceName)
    {
        if (instanceName == null)
        {
            serviceInstanceName = SERVICE_NAME;
        }
        else
        {
            serviceInstanceName = instanceName;
        }
        if (sched != null)
        {
            sched.setName(serviceInstanceName.getDisplayName());
        }
    }

    private static class ActionScheduler<K extends Comparable<K>, V extends Action<K>> extends Thread
    {
        private final GenericTimer<K, V> container;

        ActionScheduler(GenericTimer<K, V> containerRef)
        {
            container = containerRef;
        }

        @Override
        public void run()
        {
            synchronized (container)
            {
                while (!container.stopFlag)
                {
                    long waitTime = 0;
                    container.schedWakeupTime = 0;
                    {
                        // Select the timer entry with the earliest wakeup time
                        Map.Entry<Long, TreeMap<K, V>> timerEntry = container.timerMap.firstEntry();
                        long currentTime = 0;
                        while (timerEntry != null)
                        {
                            Long wakeupTime = timerEntry.getKey();
                            if (currentTime < wakeupTime)
                            {
                                currentTime = System.currentTimeMillis();
                            }

                            if (currentTime >= wakeupTime)
                            {
                                // Since the timerMap could have changed since the firstEntry() call,
                                // pollFirstEntry() will remove the same element that firstEntry() selected,
                                // and it is probably faster than calling remove()
                                container.timerMap.pollFirstEntry();

                                // Run all actions for this time slot
                                TreeMap<K, V> timeSlotMap = timerEntry.getValue();
                                for (Map.Entry<K, V> actionEntry : timeSlotMap.entrySet())
                                {
                                    K actionId = actionEntry.getKey();
                                    container.actionMap.remove(actionId);
                                    V actionObj = actionEntry.getValue();

                                    // The action object may add new timer entries
                                    actionObj.run();
                                }
                            }
                            else
                            {
                                waitTime = wakeupTime - currentTime;
                                container.schedWakeupTime = wakeupTime;
                                break;
                            }

                            // Select the next timer entry with the earliest wakeup time
                            timerEntry = container.timerMap.firstEntry();
                        }
                    }

                    try
                    {
                        container.wait(waitTime);
                    }
                    catch (InterruptedException ignored)
                    {
                    }
                }
                container.sched = null;
            }
        }
    }
}
