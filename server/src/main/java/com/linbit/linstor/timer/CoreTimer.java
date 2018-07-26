package com.linbit.linstor.timer;

import com.linbit.NegativeTimeException;
import com.linbit.SystemService;
import com.linbit.ValueOutOfRangeException;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

/**
 * linstor core timer service interface
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CoreTimer extends Timer<String, Action<String>>, SystemService
{
    @Override
    void addDelayedAction(Long delay, Action<String> actionObj)
        throws NegativeTimeException, ValueOutOfRangeException;

    @Override
    void addScheduledAction(Long scheduledTime, Action<String> actionObj);

    @Override
    void cancelAction(String actionId);
}
