package com.linbit.linstor.transaction;

import com.linbit.linstor.ControllerDatabase;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;

public class ControllerTransactionMgr implements TransactionMgr
{
    private final ControllerDatabase controllerDatabase;
    private final Connection dbCon;
    private final TransactionObjectCollection transactionObjectCollection;

    @Inject
    public ControllerTransactionMgr(ControllerDatabase controllerDatabaseRef) throws SQLException
    {
        controllerDatabase = controllerDatabaseRef;
        dbCon = controllerDatabaseRef.getConnection();
        dbCon.setAutoCommit(false);
        transactionObjectCollection = new TransactionObjectCollection();
    }

    @Override
    public void register(TransactionObject transObj)
    {
        transactionObjectCollection.register(transObj);
    }

    @Override
    public void commit() throws SQLException
    {
        dbCon.commit();

        transactionObjectCollection.commitAll();

        clearTransactionObjects();
    }


    @Override
    public void rollback() throws SQLException
    {
        transactionObjectCollection.rollbackAll();

        dbCon.rollback();

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
