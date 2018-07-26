package com.linbit.linstor.transaction;

import java.sql.Connection;
import java.sql.SQLException;

public interface TransactionMgr
{
    void register(TransactionObject transObj);

    void commit() throws SQLException;
    void rollback() throws SQLException;

    void clearTransactionObjects();
    boolean isDirty();
    int sizeObjects();

    Connection getConnection();

    void returnConnection();

    static boolean isCalledFromTransactionMgr(String methodName)
    {
        StackTraceElement[] stack = new Throwable().getStackTrace();
        boolean ret = false;
        for (StackTraceElement elem : stack)
        {
            String className = elem.getClassName();
            if (
                (
                    className.equals(TransactionMgr.class.getName()) ||
                    className.equals(ControllerTransactionMgr.class.getName()) ||
                    className.equals(SatelliteTransactionMgr.class.getName())
                ) &&
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
