package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DummyTxObj implements TransactionObject, Comparable<DummyTxObj>
{
    private final static AtomicInteger GLOB_ID = new AtomicInteger();

    private final int id = GLOB_ID.incrementAndGet();

    private Map<String, Integer> methodCalls = new TreeMap<>();

    private void call(String methodName)
    {
        Integer count = methodCalls.get(methodName);
        if (count == null)
        {
            methodCalls.put(methodName, 1);
        }
        else
        {
            methodCalls.put(methodName, count + 1);
        }
    }

    @Override
    public void setConnection(TransactionMgr transMgrRef) throws ImplementationError
    {
        call("setConnection");
    }

    @Override
    public boolean hasTransMgr()
    {
        call("hasTransMgr");
        return true;
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        call("isDirtyWithoutTransMgr");
        return false;
    }

    @Override
    public boolean isDirty()
    {
        call("isDirty");
        return false;
    }

    @Override
    public void rollback()
    {
        call("rollback");
    }

    @Override
    public void commit()
    {
        call("commit");
    }

    @Override
    public int compareTo(DummyTxObj oRef)
    {
        return Integer.compare(id, oRef.id);
    }
}
