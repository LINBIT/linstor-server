package com.linbit.linstor;

import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Optional;

public interface FreeSpaceTracker extends TransactionObject
{
    FreeSpaceMgrName getName();

    void add(AccessContext accCtx, StorPool storPool) throws AccessDeniedException;

    void remove(AccessContext accCtx, StorPool storPool) throws AccessDeniedException;

    void vlmCreating(AccessContext accCtx, Volume vlm) throws AccessDeniedException;

    void vlmCreationFinished(AccessContext accCtx, Volume vlm, long freeSpaceRef) throws AccessDeniedException;

    Optional<Long> getFreeSpaceLastUpdated(AccessContext accCtx) throws AccessDeniedException;

    long getReservedCapacity(AccessContext accCtx) throws AccessDeniedException;

    void setFreeSpace(AccessContext accCtx, long freeSpaceRef) throws AccessDeniedException;
}
