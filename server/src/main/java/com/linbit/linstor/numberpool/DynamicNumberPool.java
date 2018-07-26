package com.linbit.linstor.numberpool;

import java.util.regex.Pattern;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;

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

    int autoAllocate()
        throws ExhaustedPoolException;

    void deallocate(int nr);
}
