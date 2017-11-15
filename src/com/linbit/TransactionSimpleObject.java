package com.linbit;

import java.sql.SQLException;
import java.util.Objects;

public class TransactionSimpleObject<PARENT, ELEMENT> implements TransactionObject
{
    private boolean initialized = false;

    private PARENT parent;
    private ELEMENT object;
    private ELEMENT cachedObject;
    private SingleColumnDatabaseDriver<PARENT, ELEMENT> dbDriver;

    private TransactionMgr transMgr;

    public TransactionSimpleObject(PARENT parent, ELEMENT obj, SingleColumnDatabaseDriver<PARENT, ELEMENT> driver)
    {
        this.parent = parent;
        object = obj;
        cachedObject = obj;
        if (driver == null)
        {
            dbDriver = new NoOpObjectDatabaseDriver<>();
        }
        else
        {
            dbDriver = driver;
        }
    }

    public void set(ELEMENT obj) throws SQLException
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
        object = obj;
    }

    public ELEMENT get()
    {
        return object;
    }

    @Override
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public boolean isInitialized()
    {
        return initialized;
    }

    @Override
    public void setConnection(TransactionMgr transMgrRef) throws ImplementationError
    {
        if (isDbCacheDirty())
        {
            throw new ImplementationError("setConnection was called AFTER data was manipulated", null);
        }
        if (transMgrRef != null)
        {
            transMgrRef.register(this);
        }
        transMgr = transMgrRef;
    }

    @Override
    public void commit()
    {
        cachedObject = object;
    }

    @Override
    public void rollback()
    {
        object = cachedObject;
    }

    @Override
    public boolean isDirty()
    {
        return object != cachedObject;
    }

    @Override
    public boolean isDbCacheDirty()
    {
        return !(dbDriver instanceof NoOpObjectDatabaseDriver) && isDirty();
    }

    @Override
    public boolean hasTransMgr()
    {
        return transMgr != null;
    }
}
