package com.linbit.linstor.api.pojo;

import com.linbit.linstor.core.apis.KvsApi;

import java.util.Map;

public class KeyValueStorePojo implements KvsApi
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
