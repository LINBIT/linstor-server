package com.linbit.linstor.timer;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.linbit.timer.Action;
import com.linbit.timer.GenericTimer;

/**
 * linstor core timer service
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class CoreTimerImpl extends GenericTimer<String, Action<String>> implements CoreTimer
{
    @Inject
    public CoreTimerImpl()
    {
    }
}
