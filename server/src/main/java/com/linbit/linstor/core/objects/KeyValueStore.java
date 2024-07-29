package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.KeyValueStorePojo;
import com.linbit.linstor.api.prop.LinStorObject;
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
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class KeyValueStore extends AbsCoreObj<KeyValueStore> implements ProtectedObject
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final ObjectProtection objProt;
    private final KeyValueStoreName kvsName;
    private final Props props;
    private final KeyValueStoreDatabaseDriver driver;

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
        super(uuidRef, transObjFactory, transMgrProvider);
        objProt = objProtRef;
        kvsName = kvsNameRef;
        driver = driverRef;

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(kvsNameRef),
            toStringImpl(),
            LinStorObject.KVS
        );

        transObjs = Arrays.asList(
            objProt,
            props,
            deleted
        );
    }

    @Override
    public int compareTo(KeyValueStore keyValueStore)
    {
        return this.getName().compareTo(keyValueStore.getName());
    }

    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(kvsName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof KeyValueStore)
        {
            KeyValueStore other = (KeyValueStore) obj;
            other.checkDeleted();
            ret = Objects.equals(kvsName, other.kvsName);
        }
        return ret;
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

    @Override
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

    public KvsApi getApiData(AccessContext accCtx, @Nullable Long fullSyncId, @Nullable Long updateId)
        throws AccessDeniedException
    {
        return new KeyValueStorePojo(
            getName().getDisplayName(),
            props.map()
        );
    }

    @Override
    protected String toStringImpl()
    {
        return "KeyValueStore '" + kvsName + "'";
    }
}
