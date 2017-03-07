package com.linbit.drbdmanage;

import com.linbit.fsevent.FileSystemWatch;
import com.linbit.timer.Action;
import com.linbit.timer.Timer;

/**
 * Interface for fetching drbdmanage core services
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface CoreServices
{
    ErrorReporter getErrorReporter();

    Timer<String, Action<String>> getTimer();

    FileSystemWatch getFsWatch();
}
