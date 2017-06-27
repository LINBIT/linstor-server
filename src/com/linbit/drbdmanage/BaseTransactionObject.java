package com.linbit.drbdmanage;

import java.sql.Connection;
import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;

public abstract class BaseTransactionObject implements TransactionObject
{
    private boolean initialized = false;
    protected List<TransactionObject> transObjs;
    protected Connection dbCon;

    @Override
    public void initialized()
    {
        if (!initialized)
        {
            initialized = true;
            for(TransactionObject transObj : transObjs)
            {
                transObj.initialized();
            }
        }
    }

    @Override
    public void setConnection(TransactionMgr transMgr) throws ImplementationError
    {
        if (transMgr != null)
        {
            transMgr.register(this);
            dbCon = transMgr.dbCon;
        }
        else
        {
            dbCon = null;
        }
        for (TransactionObject transObj : transObjs)
        {
            transObj.setConnection(transMgr);
        }
    }

    @Override
    public void commit()
    {
        for (TransactionObject transObj : transObjs)
        {
            if (transObj.isDirty())
            {
                transObj.commit();
            }
        }
    }

    @Override
    public void rollback()
    {
        for (TransactionObject transObj : transObjs)
        {
            if (transObj.isDirty())
            {
                transObj.rollback();
            }
        }
    }

    @Override
    public boolean isDirty()
    {
        boolean dirty = false;
        for (TransactionObject transObj : transObjs)
        {
            if (transObj.isDirty())
            {
                dirty = true;
                break;
            }
        }
        return dirty;
    }
}
