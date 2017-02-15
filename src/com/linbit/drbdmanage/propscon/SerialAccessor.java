package com.linbit.drbdmanage.propscon;

import java.util.concurrent.locks.Lock;

/**
 * Access interface for serial numbers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SerialAccessor
{
    long getSerial();
    void setSerial(long serial);
}
