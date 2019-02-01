package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

public class KeyValueStoreData extends BaseTransactionObject implements KeyValueStore
{
    private final UUID uuid;
    private final ObjectProtection objProt;
    private final KeyValueStoreName kvsName;
    private final Props props;

    private final TransactionSimpleObject<KeyValueStoreData, Boolean> deleted;

    public KeyValueStoreData(
        UUID uuidRef,
        ObjectProtection objProtRef,
        KeyValueStoreName kvsNameRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
        throws SQLException
    {
        super(transMgrProvider);
        uuid = uuidRef;
        objProt = objProtRef;
        kvsName = kvsNameRef;

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(kvsNameRef)
        );
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            objProt,
            props,
            deleted
        );
    }

    @Override
    public int compareTo(@Nonnull KeyValueStore keyValueStore)
    {
        return this.getName().compareTo(keyValueStore.getName());
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return uuid;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public KeyValueStoreName getName()
    {
        checkDeleted();
        return kvsName;
    }

    @Override
    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, props);
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted KeyValueStore");
        }
    }
}
