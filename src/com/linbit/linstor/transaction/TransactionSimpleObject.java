package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.NoOpObjectDatabaseDriver;
import com.linbit.SingleColumnDatabaseDriver;

import java.sql.SQLException;
import java.util.Objects;

import javax.inject.Provider;

public class TransactionSimpleObject<PARENT, ELEMENT> extends AbsTransactionObject
{
    private PARENT parent;
    private ELEMENT object;
    private ELEMENT cachedObject;
    private SingleColumnDatabaseDriver<PARENT, ELEMENT> dbDriver;

    public TransactionSimpleObject(
        PARENT parentRef,
        ELEMENT objRef,
        SingleColumnDatabaseDriver<PARENT, ELEMENT> driverRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        parent = parentRef;
        object = objRef;
        cachedObject = objRef;
        if (driverRef == null)
        {
            dbDriver = new NoOpObjectDatabaseDriver<>();
        }
        else
        {
            dbDriver = driverRef;
        }
    }

    public ELEMENT set(ELEMENT obj) throws SQLException
    {
        if (isInitialized())
        {
            if (!Objects.equals(obj, cachedObject))
            {
                activateTransMgr();
                dbDriver.update(parent, obj);
            }
        }
        else
        {
            cachedObject = obj;
        }
        ELEMENT oldObj = object;
        object = obj;
        return oldObj;
    }

    public ELEMENT get()
    {
        return object;
    }

    @Override
    public void postSetConnection(TransactionMgr transMgrRef) throws ImplementationError
    {
        // forward transaction manager to simple object
        if (object instanceof TransactionObject)
        {
            ((TransactionObject) object).setConnection(transMgrRef);
        }
    }

    @Override
    public void commitImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        cachedObject = object;
    }

    @Override
    public void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        object = cachedObject;
    }

    @Override
    public boolean isDirty()
    {
        return object != cachedObject;
    }
}
