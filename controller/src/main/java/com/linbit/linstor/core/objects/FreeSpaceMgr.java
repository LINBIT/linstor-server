package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.FreeSpaceMgrName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

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

    private final TransactionSimpleObject<FreeSpaceMgr, Long> freeCapacity;
    private final TransactionSimpleObject<FreeSpaceMgr, Long> totalCapacity;

    private final TransactionSet<FreeSpaceMgr, VlmProviderObject<Resource>> pendingVolumesToAdd;
    private final TransactionSet<FreeSpaceMgr, VlmProviderObject<Snapshot>> pendingSnapshotVolumesToAdd;

    public FreeSpaceMgr(
        AccessContext privCtxRef,
        ObjectProtection objProtRef,
        FreeSpaceMgrName freeSpaceMgrNameRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactory
    )
    {
        super(transMgrProviderRef);
        privCtx = privCtxRef;
        objProt = objProtRef;
        sharedPoolName = freeSpaceMgrNameRef;
        sharedStoragePools = new TreeSet<>();

        freeCapacity = transObjFactory.createTransactionSimpleObject(this, null, null);
        totalCapacity = transObjFactory.createTransactionSimpleObject(this, null, null);
        pendingVolumesToAdd = transObjFactory.createTransactionSet(this, new TreeSet<>(), null);
        pendingSnapshotVolumesToAdd = transObjFactory.createTransactionSet(this, new TreeSet<>(), null);
        transObjs = Arrays.asList(
            freeCapacity,
            totalCapacity,
            pendingVolumesToAdd,
            pendingSnapshotVolumesToAdd
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
     * This method should be called when a storage-volume was just created but not yet deployed
     * on the {@link Satellite}.
     *
     * Pending storage-volumes only change the outcome of {@link #getFreeSpaceCurrentEstimation()}
     * but not of {@link #getFreeSpaceLastUpdated()}.
     *
     * @param vlm
     */
    @SuppressWarnings("unchecked")
    @Override
    public void vlmCreating(AccessContext accCtx, VlmProviderObject<?> vlm) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        // TODO: add check if vlm is part of a registered storPool

        if (vlm.getVolume() instanceof Volume)
        {
            synchronizedAdd(pendingVolumesToAdd, (VlmProviderObject<Resource>) vlm);
        }
        else
        {
            synchronizedAdd(pendingSnapshotVolumesToAdd, (VlmProviderObject<Snapshot>) vlm);
        }
    }

    /**
     * This method is called just to make sure that the reference to a soon deleted volume from this
     * {@link FreeSpaceMgr} are cleaned up
     * @throws AccessDeniedException
     */
    @SuppressWarnings("unchecked")
    @Override
    public void ensureVlmNoLongerCreating(AccessContext accCtx, VlmProviderObject<?> vlm)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);
        // no need to update capacity or free space as we are only deleting possible references
        // from the pendingAdding list. The "estimated space" will no longer consider this volume
        // and thus will "free up" the until now reserved space.
        if (vlm.getVolume() instanceof Volume)
        {
            synchronizedRemove(pendingVolumesToAdd, (VlmProviderObject<Resource>) vlm);
        }
        else
        {
            synchronizedRemove(pendingSnapshotVolumesToAdd, (VlmProviderObject<Snapshot>) vlm);
        }
    }

    /**
     * The given volume is removed from the pending list, and the freespace is updated.
     *
     * This method changes the outcome of both {@link #getFreeSpaceCurrentEstimation()} and
     * {@link #getFreeSpaceLastUpdated()}.
     * To be more precise, a call of this method followed atomically by a call of
     * {@link #getFreeSpaceLastUpdated()} returns <code>freeSpaceRef</code>
     *  @param vlm
     * @param freeCapacityRef
     * @param totalCapacityRef
     */
    @SuppressWarnings("unchecked")
    @Override
    public void vlmCreationFinished(
        AccessContext accCtx,
        VlmProviderObject<?> vlm,
        Long freeCapacityRef,
        Long totalCapacityRef
    )
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        if (vlm.getVolume() instanceof Volume)
        {
            synchronizedRemove(pendingVolumesToAdd, (VlmProviderObject<Resource>) vlm);
        }
        else
        {
            synchronizedRemove(pendingSnapshotVolumesToAdd, (VlmProviderObject<Snapshot>) vlm);
        }

        if (freeCapacityRef != null && totalCapacityRef != null)
        {
            setImpl(freeCapacityRef, totalCapacityRef);
        }
    }

    /**
     * @param accCtx
     * @return the last received free space size (or {@link Optional#empty()} if not initialized yet).
     * This value will not include the changes of pending adds or removes.
     *
     * @throws AccessDeniedException
     */
    @Override
    public Optional<Long> getFreeCapacityLastUpdated(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return Optional.ofNullable(freeCapacity.get());
    }

    @Override
    public Optional<Long> getTotalCapacity(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        return Optional.ofNullable(totalCapacity.get());
    }

    /**
     * @param accCtx
     * @return the currently estimated free space size (or {@link Optional#empty()} if not initialized yet).
     * This value includes the changes of pending adds or removes.
     *
     * @throws AccessDeniedException
     */
    @Override
    public long getReservedCapacity(AccessContext accCtx) throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.VIEW);

        long sum = 0;
        HashSet<VlmProviderObject<?>> pendingAddVlmCopy;
        synchronized (pendingVolumesToAdd)
        {
            pendingAddVlmCopy = new HashSet<>(pendingVolumesToAdd);
        }
        synchronized (pendingSnapshotVolumesToAdd)
        {
            pendingAddVlmCopy.addAll(pendingSnapshotVolumesToAdd);
        }
        for (VlmProviderObject<?> vlm : pendingAddVlmCopy)
        {
            sum += vlm.getAllocatedSize();
        }
        return sum;
    }

    @Override
    public void setCapacityInfo(AccessContext accCtx, long freeSpaceRef, long totalCapacityRef)
        throws AccessDeniedException
    {
        objProt.requireAccess(accCtx, AccessType.USE);

        setImpl(freeSpaceRef, totalCapacityRef);
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

    private void setImpl(long freeCapacityRef, long totalCapacityRef)
    {
        try
        {
            freeCapacity.set(freeCapacityRef);
            totalCapacity.set(totalCapacityRef);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ImplementationError("Updating free space should not throw an sql exception", sqlExc);
        }
    }
}
