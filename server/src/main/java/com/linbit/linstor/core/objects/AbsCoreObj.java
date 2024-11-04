package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectCollection;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.UUID;

public abstract class AbsCoreObj<C extends AbsCoreObj<C>> extends BaseTransactionObject
    implements Comparable<C>, DbgInstanceUuid
{
    // Object identifier
    protected final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    protected final TransactionSimpleObject<AbsCoreObj<C>, Boolean> deleted;

    public AbsCoreObj(
        UUID uuidRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        objId = uuidRef;
        dbgInstanceId = UUID.randomUUID();
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    protected void checkDeleted()
    {
        if (TransactionObjectCollection.isCheckDeletedEnabled() && deleted.get())
        {
            throw new AccessToDeletedDataException(
                "Access to deleted " + this.getClass().getSimpleName(),
                null,
                null,
                null,
                toStringImpl()
            );
        }
    }

    @Override
    public String toString()
    {
        checkDeleted();
        return toStringImpl();
    }

    public abstract void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException;

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    protected abstract String toStringImpl();
}
