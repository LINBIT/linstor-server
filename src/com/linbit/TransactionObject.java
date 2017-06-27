package com.linbit;

import java.sql.Connection;

/**
 * Interface for objects that can apply or undo one or multiple
 * previously performed changes.<br />
 * <br />
 * It is highly recommended to manually commit the database-transaction
 * (which is used by the {@link #setConnection(Connection)} method)
 * once all changes are done to the object(s) and if that database-commit
 * passed, call the commit method(s) of the modified object(s).
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
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
     * Sets the database connection for persisting the data to the database
     *
     * If the object was manipulated prior to this method-call
     * (e.g. the caches are not empty), an
     * {@link ImplementationError} is thrown.
     *
     * @param transMgr A wrapper for the database connection to be used for persistence
     */
    void setConnection(TransactionMgr transMgr) throws ImplementationError;

    /**
     * Commits the changes to the object (NOT to the database! )
     */
    void commit();

    /**
     * Reverts all changes made to this object since the last commit
     * or object creation
     */
    void rollback();

    /**
     * Returns true if there are any uncommited changes in this object
     * @return
     */
    boolean isDirty();
}
