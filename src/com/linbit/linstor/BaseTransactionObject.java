package com.linbit.linstor;

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
        if (transMgr != null && transMgrRef != null && transMgrRef != transMgr)
        {
            throw new ImplementationError("attempt to replace an active transMgr", null);
        }
        if (!hasTransMgr() && isDirtyWithoutTransMgr())
        {
            throw new ImplementationError("setConnection was called AFTER data was manipulated: " + this, null);
        }
        if (transMgrRef != null)
        {
            transMgrRef.register(this);
        }
        transMgr = transMgrRef;

        for(TransactionObject to : transObjs)
        {
            to.setConnection(transMgr);
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
        transMgr = null;
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
    public boolean isDirtyWithoutTransMgr()
    {
        if(!hasTransMgr())
            return false;

        boolean dirty = false;
        for (TransactionObject transObj : transObjs)
        {
            if (transObj.isDirtyWithoutTransMgr())
            {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    @Override
    public boolean hasTransMgr()
    {
        return transMgr != null;
    }
}
