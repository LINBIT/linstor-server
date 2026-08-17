package com.linbit.linstor.core.utils;

import com.linbit.linstor.core.CriticalError;
import com.linbit.linstor.logging.ErrorReporter;

public class ThreadUtils
{
    /*
     * register CriticalError die error handler
     * We cannot directly call System.exit on a Critical error, because the code calling the exit
     * can still have locks and the applicationmanager also needs locks for a prober shutdown
     */
    public static void setDefaultUncaughtExceptionHandler(ErrorReporter errorLog)
    {
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) ->
        {
            if (throwable instanceof CriticalError criticalError)
            {
                CriticalError.die(errorLog, criticalError);
            }
            else
            {
                errorLog.logError(
                    "Thread %s threw an uncaught exception. Thread/service might be dead or no longer responding",
                    thread.getName()
                );
                errorLog.reportError(throwable);
            }
        });
    }

    private ThreadUtils()
    {
        // utils class
    }
}
