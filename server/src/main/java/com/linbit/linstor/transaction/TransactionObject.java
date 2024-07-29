package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.transaction.manager.TransactionMgr;

public interface TransactionObject
{
    /**
     * Sets the database connection for persisting the data to the database
     *
     * If the object was manipulated prior to this method-call
     * (e.g. the caches are not empty), an
     * {@link ImplementationError} is thrown.
     *
     * @param transMgr A wrapper for the database connection to be used for persistence
     */
    void setConnection(@Nullable TransactionMgr transMgrRef) throws ImplementationError;

    /**
     * Returns true if the object has an active {@link TransactionMgr}
     */
    boolean hasTransMgr();

    /**
     * Returns true if there are any uncommited changes in this object
     * and a transaction manager is set.
     * @return
     */
    boolean isDirtyWithoutTransMgr();

    /**
     * Returns true if there are any uncommited changes in this object
     * @return
     */
    boolean isDirty();

    /**
     * Reverts all changes made to this object since the last commit
     * or object creation
     */
    void rollback();

    /**
     * Commits the changes to the object (NOT to the database! )
     */
    void commit();

}
