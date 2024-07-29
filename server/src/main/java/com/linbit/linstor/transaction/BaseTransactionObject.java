package com.linbit.linstor.transaction;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.List;

public abstract class BaseTransactionObject extends AbsTransactionObject
{
    protected @Nullable List<TransactionObject> transObjs;

    public BaseTransactionObject(Provider<? extends TransactionMgr> transMgrProvider)
    {
        super(transMgrProvider);
    }

    @Override
    public void commitImpl()
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
    public void rollbackImpl()
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
