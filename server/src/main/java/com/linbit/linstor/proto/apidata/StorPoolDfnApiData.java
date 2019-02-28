package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.proto.common.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.common.StorPoolDfnOuterClass;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class StorPoolDfnApiData implements StorPoolDefinition.StorPoolDfnApi
{
    private StorPoolDfnOuterClass.StorPoolDfn storPoolDfn;

    public StorPoolDfnApiData(StorPoolDfnOuterClass.StorPoolDfn refStorPoolDfn)
    {
        storPoolDfn = refStorPoolDfn;
    }

    @Override
    public UUID getUuid()
    {
        UUID uuid = null;
        if (storPoolDfn.hasUuid())
        {
            uuid = UUID.fromString(storPoolDfn.getUuid());
        }
        return uuid;
    }

    @Override
    public String getName()
    {
        return storPoolDfn.getStorPoolName();
    }

    @Override
    public Map<String, String> getProps()
    {
        Map<String, String> propsMap = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : storPoolDfn.getPropsList())
        {
            propsMap.put(entry.getKey(), entry.getValue());
        }
        return propsMap;
    }
}
