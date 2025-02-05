package com.linbit.linstor.transaction;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.noop.NoOpObjectDatabaseDriver;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Objects;

public class TransactionSimpleObject<PARENT, ELEMENT> extends AbsTransactionObject
{
    private final @Nullable PARENT parent;
    private @Nullable ELEMENT object;
    private @Nullable ELEMENT cachedObject;
    private final SingleColumnDatabaseDriver<PARENT, ELEMENT> dbDriver;
    private boolean dirty = false;

    /**
     * @param parentRef
     *     is only allowed to be <code>null</code> iff <code>driverRef</code> is also <code>null</code>.
     *     Otherwise, the {@link SingleColumnDatabaseDriver} might run into {@link NullPointerException}s
     * @param objRef
     *     The object that this {@link TransactionSimpleObject} should be initialized with
     * @param driverRef
     *     Optional. The {@link SingleColumnDatabaseDriver} to use.
     * @param transMgrProviderRef
     *     the {@link TransactionMgr} provider
     */
    public TransactionSimpleObject(
        @Nullable PARENT parentRef,
        @Nullable ELEMENT objRef,
        @Nullable SingleColumnDatabaseDriver<PARENT, ELEMENT> driverRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        parent = parentRef;
        object = objRef;
        cachedObject = objRef;
        if (driverRef == null)
        {
            dbDriver = new NoOpObjectDatabaseDriver<>();
        }
        else
        {
            if (parentRef == null)
            {
                throw new ImplementationError("Parent must not be null when using a database driver!");
            }
            dbDriver = driverRef;
        }
    }

    public @Nullable ELEMENT set(@Nullable ELEMENT obj) throws DatabaseException
    {
        @Nullable ELEMENT oldObj = object;
        if (!Objects.equals(obj, cachedObject))
        {
            activateTransMgr();
            object = obj;
            dbDriver.update(parent, oldObj);
            dirty = true;
        }
        else
        {
            object = obj;
        }
        return oldObj;
    }

    public @Nullable ELEMENT get()
    {
        return object;
    }

    @Override
    public void postSetConnection(@Nullable TransactionMgr transMgrRef) throws ImplementationError
    {
        // forward transaction manager to simple object
        if (object instanceof TransactionObject)
        {
            ((TransactionObject) object).setConnection(transMgrRef);
        }
    }

    @Override
    public void commitImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("commit"));
        cachedObject = object;
        dirty = false;
    }

    @Override
    public void rollbackImpl()
    {
        assert (TransactionMgr.isCalledFromTransactionMgr("rollback"));
        object = cachedObject;
        dirty = false;
    }

    @Override
    public boolean isDirty()
    {
        return dirty;
    }

    @Override
    public String toString()
    {
        return "TransactionSimpleObject [" + object + "]";
    }
}
