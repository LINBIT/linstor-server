package com.linbit.linstor.api.pojo;

import com.linbit.linstor.KeyValueStore;

public class KeyValueStorePojo implements KeyValueStore.KvsApi
{
    private final String kvsName;

    public KeyValueStorePojo(final String kvsNameRef)
    {
        kvsName = kvsNameRef;
    }

    @Override
    public String getName()
    {
        return kvsName;
    }
}
