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

    void addingVolume(AccessContext accCtx, Volume vlm) throws AccessDeniedException;

    void volumeAdded(AccessContext accCtx, Volume vlm, long freeSpaceRef) throws AccessDeniedException;

    void removingVolume(AccessContext accCtx, Volume vlm) throws AccessDeniedException;

    void volumeRemoved(AccessContext accCtx, Volume vlm, long freeSpaceRef) throws AccessDeniedException;

    Optional<Long> getFreeSpaceLastUpdated(AccessContext accCtx) throws AccessDeniedException;

    Optional<Long> getFreeSpaceCurrentEstimation(AccessContext accCtx) throws AccessDeniedException;

    void setFreeSpace(AccessContext accCtx, long freeSpaceRef) throws AccessDeniedException;
}
