package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.NoOpObjectDatabaseDriver;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;

import javax.inject.Provider;
import java.util.Objects;

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

    public ELEMENT set(ELEMENT obj) throws DatabaseException
    {
        if (!Objects.equals(obj, cachedObject))
        {
            activateTransMgr();
            dbDriver.update(parent, obj);
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
        return !Objects.equals(object, cachedObject);
    }

    @Override
    public String toString()
    {
        return "TransactionSimpleObject [" + object.toString() + "]";
    }
}
