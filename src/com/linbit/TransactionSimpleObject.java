package com.linbit;

import java.sql.SQLException;
import java.util.Objects;

public class TransactionSimpleObject<PARENT, ELEMENT> extends AbsTransactionObject
{
    private boolean initialized = false;

    private PARENT parent;
    private ELEMENT object;
    private ELEMENT cachedObject;
    private SingleColumnDatabaseDriver<PARENT, ELEMENT> dbDriver;

    public TransactionSimpleObject(
        PARENT parentRef,
        ELEMENT objRef,
        SingleColumnDatabaseDriver<PARENT, ELEMENT> driverRef
    )
    {
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
        if (initialized)
        {
            if (!Objects.equals(obj, cachedObject))
            {
                dbDriver.update(parent, obj, transMgr);
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
        if (transMgrRef != null)
        {
            if (transMgrRef != transMgr)
            {
                transMgrRef.register(this);
                // forward transaction manager to simple object
                if (object instanceof AbsTransactionObject)
                {
                    ((TransactionObject) object).setConnection(transMgrRef);
                }
            }
        }
        transMgr = transMgrRef;
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
