package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;

public interface TcpPortPool
{
    /**
     * Caller must have write-locked the reconfigurationLock
     */
    void reloadRange();

    void allocate(int nr);

    int getFreeTcpPort() throws ExhaustedPoolException;
}
