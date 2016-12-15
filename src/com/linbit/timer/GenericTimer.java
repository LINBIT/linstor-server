package com.linbit.timer;

import com.linbit.NegativeTimeException;
import com.linbit.ValueOutOfRangeException;
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
{
    private static final boolean ENABLE_DEBUG = false;

    // Maps interrupt time value to action id & action object
    private final TreeMap<Long, TreeMap<K, V>> timerMap;

    // Maps action id to interrupt time value
    private final TreeMap<K, Long> actionMap;

    private boolean stopFlag = false;

    private long schedWakeupTime = 0;

    private ActionScheduler<K, V> sched;

    /**
     * Constructs a new timer instance
     */
    public GenericTimer()
    {
        timerMap = new TreeMap<>();
        actionMap = new TreeMap<>();
        sched = null;
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
    public void addDelayedAction(Long delay, V actionObj)
        throws NegativeTimeException, ValueOutOfRangeException
    {
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "ENTER addDelayedAction()");
        }
        if (delay < 0)
        {
            if (ENABLE_DEBUG)
            {
                debugOut(GenericTimer.class, "addDelayedAction(): delay < 0");
            }
            throw new NegativeTimeException();
        }
        else
        if (delay == 0)
        {
            if (ENABLE_DEBUG)
            {
                debugOut(GenericTimer.class, "addDelayedAction(): delay == 0");
            }
            // Action without a delay triggers immediately
            // To be consistent with scheduled execution, take this timer's lock
            synchronized (this)
            {
                if (ENABLE_DEBUG)
                {
                    debugOutFormat(GenericTimer.class, "addDelayedAction(): running action id '%s'", actionObj.getId());
                }
                actionObj.run();
            }
        }
        else
        {
            if (ENABLE_DEBUG)
            {
                debugOut(GenericTimer.class, "addDelayedAction(): delay > 0");
            }
            long currentTime = System.currentTimeMillis();
            try
            {
                Long wakeupTime = Math.addExact(currentTime, delay);
                addScheduledAction(wakeupTime, actionObj);
            }
            catch (ArithmeticException arithExc)
            {
                if (ENABLE_DEBUG)
                {
                    debugOut(GenericTimer.class, "addDelayedAction(): delay + currentTime > Long.MAX_VALUE");
                }
                throw new ValueOutOfRangeException(ValueOutOfRangeException.ViolationType.TOO_HIGH);
            }
        }
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "EXIT addScheduledAction()");
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
    public void addScheduledAction(Long scheduledTime, V actionObj)
    {
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "ENTER addScheduledAction()");
        }
        K actionId = actionObj.getId();
        synchronized (this)
        {
            if (!actionMap.containsKey(actionId))
            {
                if (ENABLE_DEBUG)
                {
                    debugOutFormat(
                        GenericTimer.class,
                        "addScheduledAction(): new entry for action id '%s' at time %d",
                        actionId, scheduledTime
                    );
                }
                TreeMap<K, V> timeSlotMap = timerMap.get(scheduledTime);
                if (timeSlotMap == null)
                {
                    if (ENABLE_DEBUG)
                    {
                        debugOutFormat(
                            GenericTimer.class,
                            "addScheduledAction(): new time slot for time %d", scheduledTime
                        );
                    }
                    timeSlotMap = new TreeMap<>();
                    timerMap.put(scheduledTime, timeSlotMap);
                }
                timeSlotMap.put(actionId, actionObj);
                actionMap.put(actionId, scheduledTime);

                if (schedWakeupTime == 0 || scheduledTime < schedWakeupTime)
                {
                    if (ENABLE_DEBUG)
                    {
                        debugOutFormat(
                            GenericTimer.class,
                            "addScheduledAction(): notify ActionScheduler thread",
                            actionId
                        );
                    }
                    notify();
                }
            }
            else
            if (ENABLE_DEBUG)
            {
                debugOutFormat(
                    GenericTimer.class,
                    "addScheduledAction(): action id '%s' is already registered",
                    actionId
                );
            }
        }
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "EXIT addScheduledAction()");
        }
    }

    /**
     * Cancels an action
     *
     * Canceled actions are guaranteed not to be performed anymore after returning from this method.
     *
     * @param actionId Action to cancel
     */
    public void cancelAction(K actionId)
    {
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "ENTER cancelTimeout()");
        }
        synchronized (this)
        {
            Long wakeupTime = actionMap.get(actionId);
            if (wakeupTime != null)
            {
                if (ENABLE_DEBUG)
                {
                    debugOutFormat(GenericTimer.class, "cancelTimeout(): cancel action id '%s'", actionId);
                }
                TreeMap<K, V> timeSlotMap = timerMap.get(wakeupTime);
                if (timeSlotMap != null)
                {
                    if (ENABLE_DEBUG)
                    {
                        debugOutFormat(
                            GenericTimer.class,
                            "cancelTimeout(): cancel entry for action id '%s' at time %d\n",
                            actionId, wakeupTime
                        );
                    }
                    timeSlotMap.remove(actionId);
                    if (timeSlotMap.isEmpty())
                    {
                        if (ENABLE_DEBUG)
                        {
                            debugOutFormat(
                                GenericTimer.class,
                                "cancelTimeout(): delete time slot for time %d\n",
                                wakeupTime
                            );
                        }
                        timerMap.remove(wakeupTime);
                        if (wakeupTime == schedWakeupTime)
                        {
                            if (ENABLE_DEBUG)
                            {
                                debugOutFormat(
                                    GenericTimer.class,
                                    "cancelTimeout(): notify ActionScheduler thread",
                                    actionId
                                );
                            }
                            notify();
                        }
                    }
                }
                actionMap.remove(actionId);
            }
            else
            {
                if (ENABLE_DEBUG)
                {
                    debugOutFormat(
                        GenericTimer.class,
                        "cancelTimeout(): action id '%s' not registered", actionId
                    );
                }
            }
        }
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "EXIT cancelTimeout()");
        }
    }

    /**
     * Starts this timer instance's ActionScheduler thread
     */
    public void start()
    {
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "ENTER start()");
        }
        synchronized (this)
        {
            if (sched == null)
            {
                if (ENABLE_DEBUG)
                {
                    debugOut(GenericTimer.class, "start(): Starting new ActionScheduler thread");
                }
                sched = new ActionScheduler<>(this);
                sched.start();
            }
            else
            {
                if (ENABLE_DEBUG)
                {
                    debugOut(GenericTimer.class, "start(): ActionScheduler already running");
                }
            }
        }
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "EXIT start()");
        }
    }

    /**
     * Shuts down this timer instance's ActionScheduler thread
     *
     * This method does not wait for the ActionScheduler thread to end.
     */
    public void shutdown()
    {
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "ENTER shutdown()");
        }
        synchronized (this)
        {
            stopFlag = true;
            notify();
        }
        if (ENABLE_DEBUG)
        {
            debugOut(GenericTimer.class, "EXIT shutdown()");
        }
    }

    private static class ActionScheduler<K extends Comparable<K>, V extends Action<K>> extends Thread
    {
        private final GenericTimer container;

        ActionScheduler(GenericTimer containerRef)
        {
            container = containerRef;
        }

        @Override
        public void run()
        {
            if (ENABLE_DEBUG)
            {
                container.debugOut(GenericTimer.class, "ENTER run()");
            }
            synchronized (container)
            {
                while (!container.stopFlag)
                {
                    long waitTime = 0;
                    container.schedWakeupTime = 0;
                    {
                        if (ENABLE_DEBUG)
                        {
                            container.debugOut(GenericTimer.class, "Checking for expired timers");
                        }
                        Map.Entry<Long, TreeMap<K, V>> timerEntry = container.timerMap.firstEntry();
                        while (timerEntry != null)
                        {
                            Long wakeupTime = timerEntry.getKey();
                            long currentTime = System.currentTimeMillis();
                            if (currentTime >= wakeupTime)
                            {
                                if (ENABLE_DEBUG)
                                {
                                    container.debugOutFormat(
                                        GenericTimer.class,
                                        "Timer expired: current time %d > timer %d => Run all actions",
                                        currentTime, wakeupTime
                                    );
                                }
                                TreeMap<K, V> timeSlotMap = timerEntry.getValue();
                                Map.Entry<K, V> actionEntry = timeSlotMap.firstEntry();
                                while (actionEntry != null)
                                {
                                    K actionId = actionEntry.getKey();
                                    timeSlotMap.remove(actionId);
                                    container.actionMap.remove(actionId);
                                    V actionObj = actionEntry.getValue();

                                    if (ENABLE_DEBUG)
                                    {
                                        container.debugOutFormat(
                                            GenericTimer.class, "Time slot %d, running action id '%s'",
                                            wakeupTime, actionId
                                        );
                                    }
                                    actionObj.run();

                                    actionEntry = timeSlotMap.firstEntry();
                                }
                            }
                            else
                            {
                                waitTime = wakeupTime - currentTime;
                                container.schedWakeupTime = wakeupTime;
                                if (ENABLE_DEBUG)
                                {
                                    container.debugOutFormat(
                                        GenericTimer.class,
                                        "Next timer: current time %d < timer %d => wait time = %d",
                                        currentTime, wakeupTime, waitTime
                                    );
                                }
                                break;
                            }
                            container.timerMap.remove(timerEntry.getKey());
                            timerEntry = container.timerMap.firstEntry();
                        }
                    }

                    boolean loopWait = false;
                    do
                    {
                        try
                        {
                            if (ENABLE_DEBUG)
                            {
                                container.debugOutFormat(
                                    GenericTimer.class,
                                    "container.wait(%d)",
                                    waitTime
                                );
                            }
                            container.wait(waitTime);
                        }
                        catch (InterruptedException ex)
                        {
                            long currentTime = System.currentTimeMillis();
                            if (container.schedWakeupTime == 0)
                            {
                                if (ENABLE_DEBUG)
                                {
                                    container.debugOutFormat(
                                        GenericTimer.class,
                                        "Interrupted while waiting for condition, resuming wait",
                                        waitTime
                                    );
                                }
                                loopWait = true;
                            }
                            else
                            {
                                if (currentTime < container.schedWakeupTime)
                                {
                                    loopWait = true;
                                    waitTime = container.schedWakeupTime - currentTime;
                                    if (ENABLE_DEBUG)
                                    {
                                        container.debugOutFormat(
                                            GenericTimer.class,
                                            "Interrupted while waiting for timeout, " +
                                            "resuming with remaining wait time = %d",
                                            waitTime
                                        );
                                    }
                                }
                            }
                        }
                    }
                    while (loopWait);
                    if (ENABLE_DEBUG)
                    {
                        container.debugOut(GenericTimer.class, "wakeup from wait()");
                    }
                }
                container.sched = null;
            }
            if (ENABLE_DEBUG)
            {
                container.debugOut(GenericTimer.class, "EXIT run()");
            }
        }
    }

    void debugOut(Class cl, String message)
    {
        System.err.println(cl.getSimpleName() + ": " + message);
    }

    void debugOutFormat(Class cl, String format, Object... args)
    {
        String message = String.format(format, args);
        System.err.println(cl.getSimpleName() + ": " + message);
    }
}
