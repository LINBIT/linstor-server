package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.Collections;
import java.util.Optional;

public class StltFreeSpaceMgr extends BaseTransactionObject implements FreeSpaceTracker
{
    private final SharedStorPoolName sharedStorPoolName;

    public StltFreeSpaceMgr(Provider<TransactionMgr> transMgrProvider, SharedStorPoolName sharedStorPoolNameRef)
    {
        super(transMgrProvider);

        transObjs = Collections.emptyList();
        sharedStorPoolName = sharedStorPoolNameRef;
    }

    @Override
    public SharedStorPoolName getName()
    {
        return sharedStorPoolName;
    }

    @Override
    public void vlmCreating(VlmProviderObject<?> vlm)
    {
        // Ignore
    }

    @Override
    public void ensureVlmNoLongerCreating(VlmProviderObject<?> vlmRef)
    {
        // Trust me, I no longer track vlmProviderObject as "creating"
    }

    @Override
    public void vlmCreationFinished(
        VlmProviderObject<?> vlm,
        Long freeCapacityRef,
        Long totalCapacityRef
    )
    {
        // I'm positive
    }


    @Override
    public Optional<Long> getFreeCapacityLastUpdated()
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }

    @Override
    public long getPendingAllocatedSum()
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }

    @Override
    public Optional<Long> getTotalCapacity()
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }

    @Override
    public void setCapacityInfo(long freeSpaceRef, long totalCapacity)
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }
}
