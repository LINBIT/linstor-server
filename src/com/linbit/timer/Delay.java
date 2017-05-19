package com.linbit.timer;

/**
 * Implements functions that delay a thread
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class Delay
{
    /**
     * Pauses the thread for the specified period of time or until the
     * thread is interrupted
     *
     * @param waitTime time to wait in milliseconds
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

    private Delay()
    {
    }
}
