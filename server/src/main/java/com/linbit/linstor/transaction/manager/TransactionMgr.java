package com.linbit.linstor.transaction.manager;

import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.regex.Pattern;

public interface TransactionMgr
{
    Pattern TX_MGR_PATTERN = Pattern.compile("(Controller(SQL|ETCD)|Satellite|)TransactionMgr|DummyTxMgr");

    void register(TransactionObject transObj);

    void commit() throws TransactionException;
    void rollback() throws TransactionException;

    void clearTransactionObjects();
    boolean isDirty();
    int sizeObjects();

    default void returnConnection()
    {
    }

    static boolean isCalledFromTransactionMgr(String methodName)
    {
        StackTraceElement[] stack = new Throwable().getStackTrace();
        boolean ret = false;
        for (StackTraceElement elem : stack)
        {
            String className = elem.getClassName();
            if (
                TX_MGR_PATTERN.matcher(className).find() &&
                elem.getMethodName().equals(methodName)
            )
            {
                ret = true;
                break;
            }
        }

        return ret;
    }
}
