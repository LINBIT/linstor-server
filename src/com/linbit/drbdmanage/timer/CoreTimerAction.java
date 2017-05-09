package com.linbit.drbdmanage.timer;

import com.linbit.timer.Action;

/**
 * Action interface for the drbdmanage core timer service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CoreTimerAction extends Action<String>
{
    /**
     * Unique identifier for this timer action
     *
     * @return Unique identifier
     */
    @Override
    public String getId();

    /**
     * The action to be performed when the timer is fired
     */
    @Override
    public void run();
}
