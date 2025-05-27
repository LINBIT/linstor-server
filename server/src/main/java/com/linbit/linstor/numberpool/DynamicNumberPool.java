package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;

/**
 * Wrapper for a {@link NumberPool} which allows the range to be reloaded.
 */
public interface DynamicNumberPool
{
    /**
     * Caller must have write-locked the reconfigurationLock
     */
    void reloadRange();

    void allocate(int nr)
        throws ValueInUseException;

    boolean tryAllocate(int nr);

    int autoAllocate()
        throws ExhaustedPoolException;

    void deallocate(int nr);

}
