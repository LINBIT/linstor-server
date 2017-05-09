package com.linbit.drbdmanage;

import com.linbit.timer.Action;
import com.linbit.timer.Timer;

/**
 * Common drbdmanage core services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CoreServices
{
    ErrorReporter getErrorReporter();

    Timer<String, Action<String>> getTimer();
}
