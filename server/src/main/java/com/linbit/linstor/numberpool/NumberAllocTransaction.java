package com.linbit.linstor.numberpool;

import com.linbit.ExhaustedPoolException;
import com.linbit.linstor.transaction.AbsTransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Provider;

/**
 * Provides transaction-safe allocation of multiple numbers from a number pool cache
 *
 * All allocated numbers are tracked to enable a rollback to deallocate all numbers previously
 * allocated within the same transaction.
 *
 * This class is not multithreading-safe. If an instance of this class is used by multiple threads,
 * method calls must be serialized.
 * However, multiple NumberAllocTransaction instances can be created for the same number pool cache,
 * and those instances can be used concurrently, each instance by another thread.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class NumberAllocTransaction extends AbsTransactionObject
{
    // For interface consistency
    private final NumberPool pool;

    Set<Integer> allocatedNumbers;

    public NumberAllocTransaction(
        NumberPool poolRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        allocatedNumbers = new TreeSet<>();
        pool = poolRef;
    }

    @Override
    public void commitImpl()
    {
        // Numbers already allocated in the pool remain allocated, so the commit is a no-op

        // Clear data only required for rollback
        allocatedNumbers.clear();
    }

    @Override
    public void rollbackImpl()
    {
        int[] nrList = new int[allocatedNumbers.size()];
        int idx = 0;
        for (int nr : nrList)
        {
            nrList[idx] = nr;
            ++idx;
        }
        pool.deallocateAll(nrList);
        allocatedNumbers.clear();
    }

    @Override
    public boolean isDirty()
    {
        return !allocatedNumbers.isEmpty();
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        return !allocatedNumbers.isEmpty() && !hasTransMgr();
    }

    public boolean allocate(int nr)
    {
        activateTransMgr();
        boolean allocFlag = pool.allocate(nr);
        if (allocFlag)
        {
            allocatedNumbers.add(nr);
        }
        return allocFlag;
    }

    public Map<Integer, Boolean> multiAllocate(List<Integer> nrList)
    {
        activateTransMgr();
        Map<Integer, Boolean> allocResult = pool.multiAllocate(nrList);
        for (Map.Entry<Integer, Boolean> nrAllocResult : allocResult.entrySet())
        {
            boolean isAllocated = nrAllocResult.getValue();
            if (isAllocated)
            {
                int nr = nrAllocResult.getKey();
                allocatedNumbers.add(nr);
            }
        }
        return allocResult;
    }

    public int autoAllocate(int rangeStart, int rangeEnd) throws ExhaustedPoolException
    {
        activateTransMgr();
        int nr = pool.autoAllocate(rangeStart, rangeEnd);
        allocatedNumbers.add(nr);
        return nr;
    }

    public int autoAllocateFromOffset(int rangeStart, int rangeEnd, int offset) throws ExhaustedPoolException
    {
        activateTransMgr();
        int nr = pool.autoAllocateFromOffset(rangeStart, rangeEnd, offset);
        allocatedNumbers.add(nr);
        return nr;
    }
}
