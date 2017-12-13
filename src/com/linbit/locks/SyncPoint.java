package com.linbit.locks;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SyncPoint
{
    void register();
    void arrive();
    void await();
}
