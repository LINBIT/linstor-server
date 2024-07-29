package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.DrbdStateChange;

import java.util.Collection;

public interface DrbdStateStore
{
    void addDrbdStateChangeObserver(DrbdStateChange obs);
    boolean isDrbdStateAvailable();
    void addObserver(ResourceObserver obs, long eventsMask);
    void removeObserver(ResourceObserver obs);

    @Nullable
    DrbdResource getDrbdResource(String name) throws NoInitialStateException;
    Collection<DrbdResource> getAllDrbdResources() throws NoInitialStateException;
}
