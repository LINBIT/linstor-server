package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;

public interface MinorNrPool
{
    /**
     * Caller must have write-locked the reconfigurationLock
     */
    void reloadRange();

    void allocate(int nr);

    int getFreeMinorNr() throws ExhaustedPoolException;
}
