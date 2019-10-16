package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.core.identifier.FreeSpaceMgrName;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Collections;
import java.util.Optional;

public class StltFreeSpaceMgr extends BaseTransactionObject implements FreeSpaceTracker
{
    private static final FreeSpaceMgrName FREE_SPACE_MGR_NAME;

    static
    {
        try
        {
            FREE_SPACE_MGR_NAME = new FreeSpaceMgrName("DummyFreeSpace");
        }
        catch (InvalidNameException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public StltFreeSpaceMgr(Provider<TransactionMgr> transMgrProvider)
    {
        super(transMgrProvider);

        transObjs = Collections.emptyList();
    }

    @Override
    public FreeSpaceMgrName getName()
    {
        return FREE_SPACE_MGR_NAME;
    }

    @Override
    public void add(AccessContext accCtx, StorPool storPool)
        throws AccessDeniedException
    {
        // Ignore
    }

    @Override
    public void remove(AccessContext accCtx, StorPool storPool)
        throws AccessDeniedException
    {
        // Ignore
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
    public long getReservedCapacity(AccessContext accCtx)
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
