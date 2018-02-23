package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;

public interface MinorNrPool
{
    /**
     * Caller must have write-locked the reconfigurationLock
     */
    void reloadRange();

    int getRangeMin();

    int getRangeMax();

    void allocate(int nr)
        throws ValueOutOfRangeException, ValueInUseException;

    int getFreeMinorNr() throws ExhaustedPoolException;
}
