package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.protobuf.ProtoUuidUtils;
import com.linbit.linstor.core.apis.StorPoolDefinitionApi;
import com.linbit.linstor.proto.common.StorPoolDfnOuterClass;

import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class StorPoolDfnApiData implements StorPoolDefinitionApi
{
    private StorPoolDfnOuterClass.StorPoolDfn storPoolDfn;

    public StorPoolDfnApiData(StorPoolDfnOuterClass.StorPoolDfn refStorPoolDfn)
    {
        storPoolDfn = refStorPoolDfn;
    }

    @Override
    public @Nullable UUID getUuid()
    {
        UUID uuid = null;
        if (storPoolDfn.hasUuid())
        {
            uuid = ProtoUuidUtils.deserialize(storPoolDfn.getUuid());
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
        return storPoolDfn.getPropsMap();
    }
}
