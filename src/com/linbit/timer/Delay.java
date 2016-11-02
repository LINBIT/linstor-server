package com.linbit.timer;

public class Delay
{
    /**
     * Pauses the thread for the specified period of time or until the
     * thread is interrupted
     *
     * @param waitTime time to wait in milliseconds
     * 
     * @author Robert Altnoeder <robert.altnoeder@linbit.com>
     */
    public static void sleep(long waitTime)
    {
        try
        {
            Thread.sleep(waitTime);
        }
        catch (InterruptedException intrExc)
        {
            // No-op; allow Thread.interrupt() to break
            // out of sleep()
        }
    }
}
