package com.linbit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

public class TransactionSimpleObject<T> implements TransactionObject
{
    private boolean initialized = false;

    private T object;
    private T cachedObject;
    private ObjectDatabaseDriver<T> dbDriver;

    private Connection con;

    public TransactionSimpleObject(T obj, ObjectDatabaseDriver<T> driver)
    {
        object = obj;
        cachedObject = obj;
        if (driver == null)
        {
            dbDriver = new NoOpObjectDatabaseDriver<T>();
        }
        else
        {
            dbDriver = driver;
        }
    }

    public void set(T obj) throws SQLException
    {
        if (initialized)
        {
            if (con != null && !Objects.equals(obj, cachedObject))
            {
                if (obj == null)
                {
                    dbDriver.delete(con, object);
                }
                else
                {
                    if (object == null)
                    {
                        dbDriver.insert(con, obj);
                    }
                    else
                    {
                        dbDriver.update(con, obj);
                    }
                }
            }
        }
        else
        {
            cachedObject = obj;
        }
        object = obj;
    }

    public T get()
    {
        return object;
    }

    @Override
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        if (transMgr != null)
        {
            transMgr.register(this);
            con = transMgr.dbCon;
        }
        else
        {
            con = null;
        }
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
