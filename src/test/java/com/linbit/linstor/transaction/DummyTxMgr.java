package com.linbit.linstor.transaction;

import com.linbit.linstor.transaction.manager.TransactionMgr;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class DummyTxMgr implements TransactionMgr
{
    private List<TransactionObject> txObjList = new ArrayList<>();

    @Override
    public void register(TransactionObject transObjRef)
    {
        txObjList.add(transObjRef);
    }

    @Override
    public void commit() throws TransactionException
    {
        forEachTx(TransactionObject::commit);
        clearTransactionObjects();
    }


    @Override
    public void rollback() throws TransactionException
    {
        forEachTx(TransactionObject::rollback);
        clearTransactionObjects();
    }

    @Override
    public void clearTransactionObjects()
    {
        txObjList.clear();
    }

    @Override
    public boolean isDirty()
    {
        boolean dirty = false;
        for (TransactionObject transObj : txObjList)
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
    public int sizeObjects()
    {
        return txObjList.size();
    }

    private void forEachTx(Consumer<TransactionObject> txObjConsumer)
    {
        for (TransactionObject txObj : txObjList)
        {
            if (txObj.isDirty())
            {
                txObjConsumer.accept(txObj);
            }
        }
    }
}
