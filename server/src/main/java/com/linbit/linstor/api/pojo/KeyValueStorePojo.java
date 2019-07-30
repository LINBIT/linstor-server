package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.objects.KeyValueStore;

import java.util.Map;

public class KeyValueStorePojo implements KeyValueStore.KvsApi
{
    private final String kvsName;
    private final Map<String, String> props;

    public KeyValueStorePojo(
        final String kvsNameRef,
        final Map<String, String> propsRef
    )
    {
        kvsName = kvsNameRef;
        props = propsRef;
    }

    @Override
    public String getName()
    {
        return kvsName;
    }

    @Override
    public Map<String, String> getProps()
    {
        return props;
    }
}
