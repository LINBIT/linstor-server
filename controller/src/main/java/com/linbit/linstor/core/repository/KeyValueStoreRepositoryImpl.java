package com.linbit.linstor.core.repository;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.objects.KeyValueStore;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Holds the singleton KeyValueStore map instance.
 */
@Singleton
public class KeyValueStoreRepositoryImpl implements KeyValueStoreRepository
{
    private final CoreModule.KeyValueStoreMap kvsMap;

    @Inject
    public KeyValueStoreRepositoryImpl(CoreModule.KeyValueStoreMap kvsMapRef)
    {
        kvsMap = kvsMapRef;
    }

    @Override
    public @Nullable KeyValueStore get(
        KeyValueStoreName kvsName
    )
    {
        return kvsMap.get(kvsName);
    }

    @Override
    public void put(KeyValueStoreName kvsName, KeyValueStore kvs)
    {
        kvsMap.put(kvsName, kvs);
    }

    @Override
    public void remove(KeyValueStoreName kvsName)
    {
        kvsMap.remove(kvsName);
    }

    @Override
    public CoreModule.KeyValueStoreMap getMapForView()
    {
        return kvsMap;
    }

}
