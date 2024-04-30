package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpObjectDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Objects;

public class TransactionSimpleObject<PARENT, ELEMENT> extends AbsTransactionObject
{
    private final PARENT parent;
    private ELEMENT object;
    private ELEMENT cachedObject;
    private final SingleColumnDatabaseDriver<PARENT, ELEMENT> dbDriver;
    private boolean dirty = false;

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
        ELEMENT oldObj = object;
        if (!Objects.equals(obj, cachedObject))
        {
            activateTransMgr();
            object = obj;
            dbDriver.update(parent, oldObj);
            dirty = true;
        }
        else
        {
            object = obj;
        }
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
        dirty = false;
    }

    @Override
    public void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        object = cachedObject;
        dirty = false;
    }

    @Override
    public boolean isDirty()
    {
        return dirty;
    }

    @Override
    public String toString()
    {
        return "TransactionSimpleObject [" + object.toString() + "]";
    }
}
