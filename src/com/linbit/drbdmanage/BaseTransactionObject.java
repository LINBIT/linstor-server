package com.linbit.drbdmanage;

import java.util.List;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;

public abstract class BaseTransactionObject implements TransactionObject
{
    private boolean initialized = false;
    protected List<TransactionObject> transObjs;
    protected TransactionMgr transMgr;

    private boolean inCommit = false;
    private boolean inRollback = false;

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

        for (TransactionObject transObj : transObjs)
        {
            transObj.setConnection(transMgrRef);
        }
    }

    @Override
    public void commit()
    {
        if (!inCommit)
        {
            inCommit = true;
            for (TransactionObject transObj : transObjs)
            {
                if (transObj.isDirty())
                {
                    transObj.commit();
                }
            }
            inCommit = false;
        }
    }

    @Override
    public void rollback()
    {
        if (!inRollback)
        {
            inRollback = true;
            for (TransactionObject transObj : transObjs)
            {
                if (transObj.isDirty())
                {
                    transObj.rollback();
                }
            }
            inRollback = false;
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

    @Override
    public boolean isDbCacheDirty()
    {
        boolean dirty = false;
        for (TransactionObject transObj : transObjs)
        {
            if (transObj.isDbCacheDirty())
            {
                dirty = true;
                break;
            }
        }
        return dirty;
    }
}
