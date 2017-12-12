package com.linbit.linstor.drbdstate;

import com.linbit.linstor.core.DrbdStateChange;

public interface DrbdStateTracker
{
    void addDrbdStateChangeObserver(DrbdStateChange obs);
    boolean isDrbdStateAvailable();
    void addObserver(ResourceObserver obs, long eventsMask);
    void removeObserver(ResourceObserver obs);
    DrbdResource getDrbdResource(String name) throws NoInitialStateException;
}
