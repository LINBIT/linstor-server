package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

/**
 * Interface for objects that can apply or undo one or multiple
 * previously performed changes.<br>
 * <br>
 * It is highly recommended to manually commit the database-transaction
 * once all changes are done to the object(s) and if that database-commit
 * passed, call the commit method(s) of the modified object(s).
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class AbsTransactionObject implements TransactionObject
{
    private static final boolean DEBUG_MODE = false;

    private final @Nullable Provider<? extends TransactionMgr> transMgrProvider;

    private @Nullable TransactionMgr activeTransMgr = null;
    private boolean inCommit = false;
    private boolean inRollback = false;

    private @Nullable StackTraceElement[] dbgActivationStackstrace;

    public AbsTransactionObject(@Nullable Provider<? extends TransactionMgr> transMgrProviderRef)
    {
        transMgrProvider = transMgrProviderRef;
    }

    /**
     * This method can be overridden in case of hierarchical data-structures where
     * "this" represents just a sub-structure but the root of the data structure should
     * be added to the TransactionMgr. This is the case for example for our {@link PropsContainer}
     * @return
     */
    protected TransactionObject getObjectToRegister()
    {
        return this;
    }

    @Override
    public final void setConnection(@Nullable TransactionMgr transMgrRef) throws ImplementationError
    {
        if (activeTransMgr != transMgrRef) // prevent cyclic .setConnection calls
        {
            if (activeTransMgr != null && transMgrRef != null)
            {
                if (DEBUG_MODE)
                {
                    System.err.println("Transcation manager was previously set by: ");
                    for (StackTraceElement ste : dbgActivationStackstrace)
                    {
                        System.err.println("\tat " + ste);
                    }
                }
                throw new ImplementationError("attempt to replace an active transMgr", null);
            }
            if (!hasTransMgr() && isDirtyWithoutTransMgr())
            {
                if (DEBUG_MODE)
                {
                    System.err.println("Transcation manager was previously set by: ");
                    for (StackTraceElement ste : dbgActivationStackstrace)
                    {
                        System.err.println("\tat " + ste);
                    }
                }
                throw new ImplementationError("setConnection was called AFTER data was manipulated: " + this, null);
            }
            if (transMgrRef != null)
            {
                if (DEBUG_MODE)
                {
                    dbgActivationStackstrace = Thread.currentThread().getStackTrace();
                }
                transMgrRef.register(getObjectToRegister());
            }

            activeTransMgr = transMgrRef;
            postSetConnection(transMgrRef);
        }
    }

    /**
     * Method which can be overridden for additional tasks after the transMgr was set
     *
     * @param transMgrRef
     */
    protected void postSetConnection(@Nullable TransactionMgr transMgrRef)
    {

    }

    @Override
    public final void commit()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        if (!inCommit)
        {
            inCommit = true;
            commitImpl();
            inCommit = false;
        }
        activeTransMgr = null;
    }

    protected final boolean inCommit()
    {
        return inCommit;
    }

    protected abstract void commitImpl();

    @Override
    public final void rollback()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        if (!inRollback)
        {
            inRollback = true;
            rollbackImpl();
            inRollback = false;
        }
        if (DEBUG_MODE)
        {
            dbgActivationStackstrace = null;
        }
        activeTransMgr = null;
    }


    protected final boolean inRollback()
    {
        return inRollback;
    }

    protected abstract void rollbackImpl();

    @Override
    public final boolean hasTransMgr()
    {
        return activeTransMgr != null;
    }

    @Override
    public boolean isDirtyWithoutTransMgr()
    {
        return !hasTransMgr() && isDirty();
    }

    protected final void activateTransMgr()
    {
        getObjectToRegister().setConnection(transMgrProvider.get());
    }
}
