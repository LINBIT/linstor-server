package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
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
    public void vlmCreating(AccessContext accCtx, Volume vlm)
        throws AccessDeniedException
    {
        // Ignore
    }

    @Override
    public void vlmCreationFinished(AccessContext accCtx, Volume vlm, long freeSpaceRef)
        throws AccessDeniedException
    {
        // Ignore
    }

    @Override
    public Optional<Long> getFreeSpaceLastUpdated(AccessContext accCtx)
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
    public void setFreeSpace(AccessContext accCtx, long freeSpaceRef)
        throws AccessDeniedException
    {
        throw new UnsupportedOperationException("Satellite does not track free space");
    }
}
