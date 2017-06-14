package com.linbit;

import java.sql.SQLException;

public class TransactionSimpleObject<T> implements TransactionObject
{
    private T object;
    private T cachedObject;
    private ObjectDatabaseDriver<T> dbDriver;

    public TransactionSimpleObject(T obj, ObjectDatabaseDriver<T> driver)
    {
        object = obj;
        cachedObject = obj;
        dbDriver = driver;
    }

    public void set(T obj) throws SQLException
    {
        if (obj == null)
        {
            dbDriver.delete(object);
        }
        else
        {
            if (object == null)
            {
                dbDriver.insert(obj);
            }
            else
            {
                dbDriver.update(obj);
            }
        }
        object = obj;
    }

    public T get()
    {
        return object;
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        transMgr.register(this);
        dbDriver.setConnection(transMgr.dbCon);
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
}
