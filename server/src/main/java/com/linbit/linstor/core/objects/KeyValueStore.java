package com.linbit.linstor.core.objects;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.KeyValueStorePojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apis.KvsApi;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class KeyValueStore extends AbsCoreObj<KeyValueStore>
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final KeyValueStoreName kvsName;
    private final Props props;
    private final KeyValueStoreDatabaseDriver driver;

    public KeyValueStore(
        UUID uuidRef,
        KeyValueStoreName kvsNameRef,
        KeyValueStoreDatabaseDriver driverRef,
        PropsContainerFactory propsContainerFactory,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
        throws DatabaseException
    {
        super(uuidRef, transObjFactory, transMgrProvider);
        kvsName = kvsNameRef;
        driver = driverRef;

        props = propsContainerFactory.getInstance(
            PropsContainer.buildPath(kvsNameRef),
            toStringImpl(),
            LinStorObject.KVS
        );

        transObjs = Arrays.asList(
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
        else if (obj instanceof KeyValueStore other)
        {
            other.checkDeleted();
            ret = Objects.equals(kvsName, other.kvsName);
        }
        return ret;
    }

    public KeyValueStoreName getName()
    {
        checkDeleted();
        return kvsName;
    }

    public Props getProps()
    {
        checkDeleted();
        return props;
    }

    @Override
    public void delete() throws DatabaseException
    {
        if (!deleted.get())
        {

            props.delete();

            activateTransMgr();

            driver.delete(this);

            deleted.set(true);
        }
    }

    public KvsApi getApiData(@Nullable Long fullSyncId, @Nullable Long updateId)
    {
        return new KeyValueStorePojo(
            getName().getDisplayName(),
            props.cloneMap()
        );
    }

    @Override
    protected String toStringImpl()
    {
        return "KeyValueStore '" + kvsName + "'";
    }
}
