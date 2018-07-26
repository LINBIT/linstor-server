package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;

import java.util.LinkedHashSet;
import java.util.Set;

public class TransactionObjectCollection
{
    private Set<TransactionObject> transObjects;

    public TransactionObjectCollection()
    {
        transObjects = new LinkedHashSet<>(); // preserves the order but removes duplicates
    }

    public void register(TransactionObject transObj)
    {
        if (transObj.isDirtyWithoutTransMgr() && !transObjects.contains(transObj))
        {
            throw new ImplementationError(
                "Connection set after TransactionObject modified " + transObj,
                null
            );
        }
        transObjects.add(transObj);
    }

    public void commitAll()
    {
        for (TransactionObject transObj : transObjects)
        {
            // checking if isDirty to prevent endless indirect recursion
            if (transObj.isDirty())
            {
                transObj.commit();
            }
        }
    }

    public void rollbackAll()
    {
        for (TransactionObject transObj : transObjects)
        {
            // checking if isDirty to prevent endless indirect recursion
            if (transObj.isDirty())
            {
                transObj.rollback();
            }
        }
    }

    public void clearAll()
    {
        for (TransactionObject transObj : transObjects)
        {
            // remove the active connection to force the next transaction to be explicit
            transObj.setConnection(null);
        }

        transObjects.clear();
    }

    public boolean areAnyDirty()
    {
        boolean dirty = false;
        for (TransactionObject transObj : transObjects)
        {
            if (transObj.isDirty())
            {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    public int sizeObjects()
    {
        return transObjects.size();
    }
}
