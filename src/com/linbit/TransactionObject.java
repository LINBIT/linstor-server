package com.linbit;

public interface TransactionObject
{
    /**
     * Until this method is called for the first time, all actions performed
     * to this object are NOT cached and NOT persisted.
     * The first call of this method enables caching and persisting.
     * Further calls have no effect.
     */
    void initialized();

    /**
     * Returns true if {@link TransactionObject#initialized()} was called
     */
    boolean isInitialized();

    /**
     * Sets the database connection for persisting the data to the database
     *
     * If the object was manipulated prior to this method-call
     * (e.g. the caches are not empty), an
     * {@link ImplementationError} is thrown.
     *
     * @param transMgr A wrapper for the database connection to be used for persistence
     */
    void setConnection(TransactionMgr transMgrRef) throws ImplementationError;

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
