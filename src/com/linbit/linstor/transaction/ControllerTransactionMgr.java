package com.linbit.linstor.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.inject.Inject;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbcp.DbConnectionPool;

public class ControllerTransactionMgr implements TransactionMgr
{
    private final boolean isSatellite;
    private final DbConnectionPool dbConnectionPool;
    private final Connection dbCon;
    private Set<TransactionObject> transObjects;

    @Inject
    public ControllerTransactionMgr(DbConnectionPool dbConnPool) throws SQLException
    {
        isSatellite = false;
        dbConnectionPool = dbConnPool;
        dbCon = dbConnPool.getConnection();
        dbCon.setAutoCommit(false);
        transObjects = new LinkedHashSet<>(); // preserves the order but removes duplicates
    }

    ControllerTransactionMgr()
    {
        isSatellite = true;
        dbConnectionPool = null;
        dbCon = null;
        transObjects = new LinkedHashSet<>(); // preserves the order but removes duplicates
    }

    @Override
    public void register(TransactionObject transObj)
    {
        if (transObj.isDirtyWithoutTransMgr() && !transObjects.contains(transObj))
        {
            throw new ImplementationError(
                "Connection set after TransactionObject modified " + transObj,
                null
            );
        }
        transObjects.add(transObj);
    }

    @Override
    public void commit() throws SQLException
    {
        if (!isSatellite)
        {
            dbCon.commit();
        }
        for (TransactionObject transObj : transObjects)
        {
            // checking if isDirty to prevent endless indirect recursion
            if (transObj.isDirty())
            {
                transObj.commit();
            }
        }

        clearTransactionObjects();
    }


    @Override
    public void rollback() throws SQLException
    {
        for (TransactionObject transObj : transObjects)
        {
            // checking if isDirty to prevent endless indirect recursion
            if (transObj.isDirty())
            {
                transObj.rollback();
            }
        }
        if (!isSatellite)
        {
            dbCon.rollback();
        }

        clearTransactionObjects();
    }

    @Override
    public void clearTransactionObjects()
    {
        // if no SQLException happened so far
        for (TransactionObject transObj : transObjects)
        {
            // remove the active connection to force the next transaction to be explicit
            transObj.setConnection(null);
        }

        transObjects.clear();
    }

    @Override
    public boolean isDirty()
    {
        boolean dirty = false;
        for (TransactionObject transObj : transObjects)
        {
            if (transObj.isDirty())
            {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    @Override
    public int sizeObjects()
    {
        return transObjects.size();
    }

    @Override
    public boolean isRegistered(TransactionObject to)
    {
        return transObjects.contains(to);
    }

    @Override
    public Connection getConnection()
    {
        return dbCon;
    }

    @Override
    public void returnConnection()
    {
        if (dbConnectionPool != null)
        {
            dbConnectionPool.returnConnection(dbCon);
        }
        clearTransactionObjects();
    }
}
