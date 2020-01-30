package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.ControllerSQLDatabase;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionObject;
import com.linbit.linstor.transaction.TransactionObjectCollection;

import java.sql.Connection;
import java.sql.SQLException;

public class ControllerSQLTransactionMgr implements TransactionMgrSQL
{
    private final ControllerSQLDatabase controllerDatabase;
    private final Connection dbCon;
    private final TransactionObjectCollection transactionObjectCollection;

    public ControllerSQLTransactionMgr(ControllerSQLDatabase controllerDatabaseRef) throws SQLException
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
