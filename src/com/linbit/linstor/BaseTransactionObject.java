package com.linbit.linstor;

import java.util.List;

import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.AbsTransactionObject;

public abstract class BaseTransactionObject extends AbsTransactionObject
{
    protected List<TransactionObject> transObjs;

    @Override
    public void initialized()
    {
        if (!isInitialized())
        {
            super.initialized();
            for (TransactionObject transObj : transObjs)
            {
                transObj.initialized();
            }
        }
    }

    @Override
    public void postSetConnection(TransactionMgr transMgr)
    {
        if (transMgr == null)
        {
            for (TransactionObject to : transObjs)
            {
                to.setConnection(transMgr);
            }
        }
        else
        {
            for (TransactionObject to : transObjs)
            {
                if (!transMgr.isRegistered(to)) // avoid infinite recursion
                {
                    to.setConnection(transMgr);
                }
            }
        }
    }

    @Override
    public void commitImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        for (TransactionObject transObj : transObjs)
        {
            if (transObj.isDirty())
            {
                transObj.commit();
            }
        }
    }

    @Override
    public void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
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

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        boolean dirty = false;
        if (hasTransMgr())
        {
            for (TransactionObject transObj : transObjs)
            {
                if (transObj.isDirtyWithoutTransMgr())
                {
                    dirty = true;
                    break;
                }
            }
        }
        return dirty;
    }
}
