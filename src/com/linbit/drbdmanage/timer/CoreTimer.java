package com.linbit.drbdmanage.timer;

import com.linbit.NegativeTimeException;
import com.linbit.SystemService;
import com.linbit.ValueOutOfRangeException;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

/**
 * drbdmanageNG core timer service interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CoreTimer extends Timer<String, Action<String>>, SystemService
{
    @Override
    public void addDelayedAction(Long delay, Action<String> actionObj)
        throws NegativeTimeException, ValueOutOfRangeException;

    @Override
    public void addScheduledAction(Long scheduledTime, Action<String> actionObj);

    @Override
    public void cancelAction(String actionId);
}
