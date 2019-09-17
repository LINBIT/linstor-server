package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.api.pojo.KeyValueStorePojo;
import com.linbit.linstor.core.apis.KvsApi;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.UUID;

public class KeyValueStore extends BaseTransactionObject implements Comparable<KeyValueStore>, ProtectedObject
{
    public static interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final UUID uuid;
    private final ObjectProtection objProt;
    private final KeyValueStoreName kvsName;
    private final Props props;
    private final KeyValueStoreDatabaseDriver driver;

    private final TransactionSimpleObject<KeyValueStore, Boolean> deleted;

    public KeyValueStore(
        UUID uuidRef,
        ObjectProtection objProtRef,
        KeyValueStoreName kvsNameRef,
        KeyValueStoreDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws DatabaseException
    {
        super(transMgrProvider);
        uuid = uuidRef;
        objProt = objProtRef;
        kvsName = kvsNameRef;
        driver = driverRef;

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

    public KeyValueStoreName getName()
    {
        checkDeleted();
        return kvsName;
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, props);
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            props.delete();
            objProt.delete(accCtx);

            activateTransMgr();

            driver.delete(this);

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted KeyValueStore");
        }
    }

    public KvsApi getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
        return new KeyValueStorePojo(
            getName().getDisplayName(),
            props.map()
        );
    }
}
