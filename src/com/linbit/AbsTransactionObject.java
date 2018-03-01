package com.linbit;

import java.sql.Connection;

/**
 * Interface for objects that can apply or undo one or multiple
 * previously performed changes.<br>
 * <br>
 * It is highly recommended to manually commit the database-transaction
 * (which is used by the {@link AbsTransactionObject#setConnection(Connection)} method)
 * once all changes are done to the object(s) and if that database-commit
 * passed, call the commit method(s) of the modified object(s).
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class AbsTransactionObject implements TransactionObject
{
    private boolean initialized = false;
    protected TransactionMgr transMgr = null;
    private boolean inCommit = false;
    private boolean inRollback = false;


    @Override
    public void initialized()
    {
        initialized = true;
    }

    @Override
    public boolean isInitialized()
    {
        return initialized;
    }

    @Override
    public final void setConnection(TransactionMgr transMgrRef) throws ImplementationError
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

        postSetConnection(transMgrRef);
    }

    /**
     * Method which can be overridden for additional tasks after the transMgr was set
     *
     * @param transMgrRef
     */
    protected void postSetConnection(TransactionMgr transMgrRef)
    {

    }

    @Override
    public final void commit()
    {
        if (!inCommit)
        {
            inCommit = true;
            commitImpl();
            inCommit = false;
        }
        transMgr = null;
    }

    protected final boolean inCommit()
    {
        return inCommit;
    }

    protected abstract void commitImpl();

    @Override
    public final void rollback()
    {
        if (!inRollback)
        {
            inRollback = true;
            rollbackImpl();
            inRollback = false;
        }
        transMgr = null;
    }


    protected final boolean inRollback()
    {
        return inRollback;
    }

    protected abstract void rollbackImpl();

    @Override
    public final boolean hasTransMgr()
    {
        return transMgr != null;
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        return !hasTransMgr() && isDirty();
    }
}
