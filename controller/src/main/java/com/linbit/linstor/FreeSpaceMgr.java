package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class FreeSpaceMgr extends BaseTransactionObject implements FreeSpaceTracker
{
    private final AccessContext privCtx;

    private final ObjectProtection objProt;

    private final FreeSpaceMgrName sharedPoolName;
    private final Set<StorPool> sharedStoragePools;

    private final TransactionSimpleObject<FreeSpaceMgr, Long> freeSpace;

    private final Set<Volume> pendingVolumesToAdd = new HashSet<>();

    public FreeSpaceMgr(
        AccessContext privCtxRef,
        ObjectProtection objProtRef,
        FreeSpaceMgrName freeSpaceMgrNameRef,
        Provider<TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactory
    )
    {
        super(transMgrProviderRef);
        privCtx = privCtxRef;
        objProt = objProtRef;
        sharedPoolName = freeSpaceMgrNameRef;
        sharedStoragePools = new TreeSet<>();

        freeSpace = transObjFactory.createTransactionSimpleObject(this, null, null);
        transObjs = Arrays.asList(
            freeSpace
        );
    }

    /**
     * @return The name of the shared pool
     */
    @Override
    public FreeSpaceMgrName getName()
    {
        return sharedPoolName;
    }

    /**
     * Adds a new {@link StorPool} to this shared storage pool.
     *
     * @param storPool
     * @throws AccessDeniedException
     */
    @Override
    public void add(AccessContext accCtx, StorPool storPool) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        // TODO: add check if vlm is part of a registered storPool
        synchronizedAdd(sharedStoragePools, storPool);
    }

    /**
     * Removes a new {@link StorPool} to this shared storage pool.
     *
     * @param storPool
     */
    @Override
    public void remove(AccessContext accCtx, StorPool storPool) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        synchronizedRemove(sharedStoragePools, storPool);
    }

    /**
     * This method should be called when a volume was just created but not yet deployed
     * on the {@link Satellite}.
     *
     * Pending volumes only change the outcome of {@link #getFreeSpaceCurrentEstimation()}
     * but not of {@link #getFreeSpaceLastUpdated()}.
     *
     * @param vlm
     */
    @Override
    public void addingVolume(AccessContext accCtx, Volume vlm) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        // TODO: add check if vlm is part of a registered storPool
        synchronizedAdd(pendingVolumesToAdd, vlm);
    }

    /**
     * The given volume is removed from the pending list, and the freespace is updated.
     *
     * This method changes the outcome of both {@link #getFreeSpaceCurrentEstimation()} and
     * {@link #getFreeSpaceLastUpdated()}.
     * To be more precise, a call of this method followed atomically by a call of
     * {@link #getFreeSpaceLastUpdated()} returns <code>freeSpaceRef</code>
     *
     * @param vlm
     * @param freeSpaceRef
     */
    @Override
    public void volumeAdded(AccessContext accCtx, Volume vlm, long freeSpaceRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        synchronizedRemove(pendingVolumesToAdd, vlm);
        setImpl(freeSpaceRef);
    }

    /**
     * @param accCtx
     * @return the last received free space size (or {@link Optional#empty()} if not initialized yet).
     * This value will not include the changes of pending adds or removes.
     *
     * @throws AccessDeniedException
     */
    @Override
    public Optional<Long> getFreeSpaceLastUpdated(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return Optional.ofNullable(freeSpace.get());
    }

    /**
     * @param accCtx
     * @return the currently estimated free space size (or {@link Optional#empty()} if not initialized yet).
     * This value includes the changes of pending adds or removes.
     *
     * @throws AccessDeniedException
     */
    @Override
    public Optional<Long> getFreeSpaceCurrentEstimation(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        Optional<Long> ret;
        Long freeSpaceTmp = freeSpace.get();
        if (freeSpaceTmp == null)
        {
            ret = Optional.empty();
        }
        else
        {
            long sum = 0;
            try
            {
                HashSet<Volume> pendingAddCopy;
                synchronized (pendingVolumesToAdd)
                {
                    pendingAddCopy = new HashSet<>(pendingVolumesToAdd);
                }

                for (Volume vlm : pendingAddCopy)
                {
                    sum += vlm.getVolumeDefinition().getVolumeSize(privCtx);
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ImplementationError("Privileged access context has not enough privileges", accDeniedExc);
            }
            ret = Optional.of(freeSpaceTmp - sum);
        }
        return ret;
    }

    @Override
    public void setFreeSpace(AccessContext accCtx, long freeSpaceRef) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        setImpl(freeSpaceRef);
    }

    @Override
    public boolean isDirty()
    {
        return freeSpace.isDirty();
    }

    @Override
    public void commitImpl()
    {
        freeSpace.commit();
    }

    @Override
    public void rollbackImpl()
    {
        freeSpace.rollback();
    }

    private <T> boolean synchronizedAdd(Set<T> set, T element)
    {
        synchronized (set)
        {
            return set.add(element);
        }
    }

    private <T> boolean synchronizedRemove(Set<T> set, T element)
    {
        synchronized (set)
        {
            return set.remove(element);
        }
    }

    private void setImpl(long freeSpaceRef)
    {
        try
        {
            freeSpace.set(freeSpaceRef);
        }
        catch (SQLException sqlExc)
        {
            throw new ImplementationError("Updating free space should not throw an sql exception", sqlExc);
        }
    }
}
