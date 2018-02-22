package com.linbit.linstor;

import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

/**
 * Common linstor core services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CoreServices
{
    ErrorReporter getErrorReporter();
}
