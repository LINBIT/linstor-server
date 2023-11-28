package com.linbit.linstor.core.objects;

import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

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
    public void vlmCreating(AccessContext accCtx, VlmProviderObject<?> vlm)
        throws AccessDeniedException
    {
        // Ignore
    }

    @Override
    public void ensureVlmNoLongerCreating(AccessContext accCtxRef, VlmProviderObject<?> vlmRef)
        throws AccessDeniedException
    {
        // Trust me, I no longer track vlmProviderObject as "creating"
    }

    @Override
    public void vlmCreationFinished(
        AccessContext accCtx,
        VlmProviderObject<?> vlm,
        Long freeCapacityRef,
        Long totalCapacityRef
    )
        throws AccessDeniedException
    {
        // I'm positive
    }


    @Override
    public Optional<Long> getFreeCapacityLastUpdated(AccessContext accCtx)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }

    @Override
    public long getPendingAllocatedSum(AccessContext accCtx)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }

    @Override
    public Optional<Long> getTotalCapacity(AccessContext accCtx)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }

    @Override
    public void setCapacityInfo(AccessContext accCtx, long freeSpaceRef, long totalCapacity)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }
}
