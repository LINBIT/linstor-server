package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Optional;

public interface FreeSpaceTracker extends TransactionObject
{
    FreeSpaceMgrName getName();

    void add(AccessContext accCtx, StorPool storPool) throws AccessDeniedException;

    void remove(AccessContext accCtx, StorPool storPool) throws AccessDeniedException;

    void vlmCreating(AccessContext accCtx, VlmProviderObject vlmProviderObjRef) throws AccessDeniedException;

    void ensureVlmNoLongerCreating(
        AccessContext accCtxRef,
        VlmProviderObject vlmProviderObjRef
    )
        throws AccessDeniedException;

    void vlmCreationFinished(
        AccessContext accCtx,
        VlmProviderObject vlmProviderObjRef,
        Long freeCapacityRef,
        Long totalCapacityRef
    )
        throws AccessDeniedException;

    Optional<Long> getFreeCapacityLastUpdated(AccessContext accCtx) throws AccessDeniedException;

    Optional<Long> getTotalCapacity(AccessContext accCtx) throws AccessDeniedException;

    long getReservedCapacity(AccessContext accCtx) throws AccessDeniedException;

    void setCapacityInfo(AccessContext accCtx, long freeSpaceRef, long totalCapacity) throws AccessDeniedException;
}
