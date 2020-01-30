package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectCollection;

public class SatelliteTransactionMgr implements TransactionMgr
{
    private final TransactionObjectCollection transactionObjectCollection;

    public SatelliteTransactionMgr()
    {
        transactionObjectCollection = new TransactionObjectCollection();
    }

    @Override
    public void register(TransactionObject transObj)
    {
        transactionObjectCollection.register(transObj);
    }

    @Override
    public void commit()
    {
        transactionObjectCollection.commitAll();
        clearTransactionObjects();
    }


    @Override
    public void rollback()
    {
        transactionObjectCollection.rollbackAll();
        clearTransactionObjects();
    }

    @Override
    public void clearTransactionObjects()
    {
        transactionObjectCollection.clearAll();
    }

    @Override
    public boolean isDirty()
    {
        return transactionObjectCollection.areAnyDirty();
    }

    @Override
    public int sizeObjects()
    {
        return transactionObjectCollection.sizeObjects();
    }

    @Override
    public void returnConnection()
    {
        clearTransactionObjects();
    }
}
