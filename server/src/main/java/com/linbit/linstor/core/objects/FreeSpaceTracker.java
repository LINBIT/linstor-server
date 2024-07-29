package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Optional;

public interface FreeSpaceTracker extends TransactionObject
{
    SharedStorPoolName getName();

    void vlmCreating(AccessContext accCtx, VlmProviderObject<?> vlmProviderObjRef) throws AccessDeniedException;

    void ensureVlmNoLongerCreating(
        AccessContext accCtxRef,
        VlmProviderObject<?> vlmProviderObjRef
    )
        throws AccessDeniedException;

    void vlmCreationFinished(
        AccessContext accCtx,
        VlmProviderObject<?> vlmProviderObjRef,
        @Nullable Long freeCapacityRef,
        @Nullable Long totalCapacityRef
    )
        throws AccessDeniedException;

    Optional<Long> getFreeCapacityLastUpdated(AccessContext accCtx) throws AccessDeniedException;

    Optional<Long> getTotalCapacity(AccessContext accCtx) throws AccessDeniedException;

    long getPendingAllocatedSum(AccessContext accCtx) throws AccessDeniedException;

    void setCapacityInfo(AccessContext accCtx, long freeSpaceRef, long totalCapacity) throws AccessDeniedException;
}
