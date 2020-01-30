package com.linbit.linstor.storage.interfaces.layers;

import com.linbit.ImplementationError;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

public class State implements TransactionObject
{
    private final boolean good;
    private final boolean stable;
    private final String descr;

    public State(boolean goodRef, boolean stableRef, String descrRef)
    {
        good = goodRef;
        stable = stableRef;
        descr = descrRef;
    }

    public boolean isGoodState()
    {
        return good;
    }

    public boolean isStable()
    {
        return stable;
    }

    @Override
    public String toString()
    {
        return descr;
    }

    @Override
    public void setConnection(TransactionMgr transMgrRef) throws ImplementationError
    {
        // no-op
    }

    @Override
    public boolean hasTransMgr()
    {
        return false;
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        return false;
    }

    @Override
    public boolean isDirty()
    {
        return false;
    }

    @Override
    public void rollback()
    {
        // no-op
    }

    @Override
    public void commit()
    {
        // no-op
    }
}
