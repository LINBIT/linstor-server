package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;

import com.linbit.linstor.dbcp.DbConnectionPool;

public class TransactionMgr
{
    private final boolean isSatellite;
    public final Connection dbCon;
    private Set<TransactionObject> transObjects;

    public TransactionMgr(DbConnectionPool dbConnPool) throws SQLException
    {
        this(dbConnPool.getConnection());
    }

    public TransactionMgr(Connection con) throws SQLException
    {
        isSatellite = false;
        con.setAutoCommit(false);
        dbCon = con;
        transObjects = new LinkedHashSet<>(); // preserves the order but removes duplicates
    }

    TransactionMgr()
    {
        isSatellite = true;
        dbCon = null;
        transObjects = new LinkedHashSet<>(); // preserves the order but removes duplicates
    }

    public void register(TransactionObject transObj)
    {
        if(transObj.isDirtyWithoutTransMgr())
        {
            throw new ImplementationError(
                "Connection set after TransactionObject modified " + transObj,
                null
            );
        }
        transObjects.add(transObj);
    }

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

    public int sizeObjects()
    {
        return transObjects.size();
    }
}
