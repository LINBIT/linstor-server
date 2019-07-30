package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerDatabase;

import java.sql.Connection;
import java.sql.SQLException;

public class ControllerTransactionMgr implements TransactionMgr
{
    private final ControllerDatabase controllerDatabase;
    private final Connection dbCon;
    private final TransactionObjectCollection transactionObjectCollection;

    public ControllerTransactionMgr(ControllerDatabase controllerDatabaseRef) throws SQLException
    {
        controllerDatabase = controllerDatabaseRef;
        dbCon = controllerDatabaseRef.getConnection();
        transactionObjectCollection = new TransactionObjectCollection();
    }

    @Override
    public void register(TransactionObject transObj)
    {
        transactionObjectCollection.register(transObj);
    }

    @Override
    public void commit() throws TransactionException
    {
        try
        {
            dbCon.commit();
        }
        catch (SQLException sqlExc)
        {
            throw new TransactionException("Database commit failed.", sqlExc);
        }

        transactionObjectCollection.commitAll();

        clearTransactionObjects();
    }


    @Override
    public void rollback() throws TransactionException
    {
        transactionObjectCollection.rollbackAll();

        try
        {
            dbCon.rollback();
        }
        catch (SQLException sqlExc)
        {
            throw new TransactionException("Database rollback failed.", sqlExc);
        }

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
        return dbCon;
    }

    @Override
    public void returnConnection()
    {
        controllerDatabase.returnConnection(dbCon);

        clearTransactionObjects();
    }
}
