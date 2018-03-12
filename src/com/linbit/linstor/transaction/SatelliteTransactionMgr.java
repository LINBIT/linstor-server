package com.linbit.linstor.transaction;

import javax.inject.Inject;
import java.sql.Connection;

public class SatelliteTransactionMgr implements TransactionMgr
{
    private final TransactionObjectCollection transactionObjectCollection;

    @Inject
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
    public Connection getConnection()
    {
        return null;
    }

    @Override
    public void returnConnection()
    {
        clearTransactionObjects();
    }
}
