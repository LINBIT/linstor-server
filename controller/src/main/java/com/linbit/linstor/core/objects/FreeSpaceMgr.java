package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSet;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class FreeSpaceMgr extends BaseTransactionObject implements FreeSpaceTracker
{
    private final SharedStorPoolName sharedPoolName;

    private final TransactionSimpleObject<FreeSpaceMgr, Long> freeCapacity;
    private final TransactionSimpleObject<FreeSpaceMgr, Long> totalCapacity;

    private final TransactionSet<FreeSpaceMgr, VlmProviderObject<Resource>> pendingVolumesToAdd;
    private final TransactionSet<FreeSpaceMgr, VlmProviderObject<Snapshot>> pendingSnapshotVolumesToAdd;

    public FreeSpaceMgr(
        SharedStorPoolName sharedStorPoolNameRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactory
    )
    {
        super(transMgrProviderRef);
        sharedPoolName = sharedStorPoolNameRef;

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
    public SharedStorPoolName getName()
    {
        return sharedPoolName;
    }

    /**
     * This method should be called when a storage-volume was just created but not yet deployed
     * on the {@link Satellite}.
     *
     * Pending storage-volumes only change the outcome of {@link #getPendingAllocatedSum()}
     * but not of {@link #getFreeCapacityLastUpdated()}.
     *
     */
    @SuppressWarnings("unchecked")
    @Override
    public void vlmCreating(VlmProviderObject<?> vlm)
    {
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
     */
    @SuppressWarnings("unchecked")
    @Override
    public void ensureVlmNoLongerCreating(VlmProviderObject<?> vlm)
    {
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
     * This method changes the outcome of both {@link #getPendingAllocatedSum()} and
     * {@link #getFreeCapacityLastUpdated()}.
     * To be more precise, a call of this method followed atomically by a call of
     * {@link #getFreeCapacityLastUpdated()} returns <code>freeSpaceRef</code>
     */
    @SuppressWarnings("unchecked")
    @Override
    public void vlmCreationFinished(
        VlmProviderObject<?> vlm,
        Long freeCapacityRef,
        Long totalCapacityRef
    )
    {
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
     * @return the last received free space size (or {@link Optional#empty()} if not initialized yet).
     * This value will not include the changes of pending adds or removes.
     *
     */
    @Override
    public Optional<Long> getFreeCapacityLastUpdated()
    {
        return Optional.ofNullable(freeCapacity.get());
    }

    @Override
    public Optional<Long> getTotalCapacity()
    {
        return Optional.ofNullable(totalCapacity.get());
    }

    /**
     * @return the currently estimated free space size (or {@link Optional#empty()} if not initialized yet).
     * This value includes the changes of pending adds or removes.
     *
     */
    @Override
    public long getPendingAllocatedSum()
    {
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
    public void setCapacityInfo(long freeSpaceRef, long totalCapacityRef)
    {
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
