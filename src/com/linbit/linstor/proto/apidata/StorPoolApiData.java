package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.StorPoolOuterClass;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class StorPoolApiData implements StorPool.StorPoolApi
{
    private StorPoolOuterClass.StorPool storPool;

    public StorPoolApiData(StorPoolOuterClass.StorPool refStorPool)
    {
        storPool = refStorPool;
    }

    @Override
    public UUID getStorPoolUuid()
    {
        UUID uuid = null;
        if (storPool.hasStorPoolUuid())
        {
            uuid = UUID.fromString(storPool.getStorPoolUuid());
        }
        return uuid;
    }

    @Override
    public String getStorPoolName()
    {
        return storPool.getStorPoolName();
    }

    @Override
    public String getNodeName()
    {
        return storPool.getNodeName();
    }

    @Override
    public UUID getNodeUuid()
    {
        UUID uuid = null;
        if (storPool.hasNodeUuid())
        {
            uuid = UUID.fromString(storPool.getNodeUuid());
        }
        return uuid;
    }

    @Override
    public UUID getStorPoolDfnUuid()
    {
        UUID uuid = null;
        if (storPool.hasStorPoolDfnUuid())
        {
            uuid = UUID.fromString(storPool.getStorPoolDfnUuid());
        }
        return uuid;
    }

    @Override
    public String getDriver()
    {
        return storPool.getDriver();
    }

    @Override
    public Map<String, String> getStorPoolProps()
    {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : storPool.getPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    @Override
    public List<Volume.VlmApi> getVlmList()
    {
        return VlmApiData.toApiList(storPool.getVlmsList());
    }

    @Override
    public Map<String, String> getStorPoolStaticTraits()
    {
        return BaseProtoApiCall.asMap(storPool.getStaticTraitsList());
    }

    public static StorPoolOuterClass.StorPool toStorPoolProto(final StorPool.StorPoolApi apiStorPool)
    {
        StorPoolOuterClass.StorPool.Builder storPoolBld = StorPoolOuterClass.StorPool.newBuilder();
        storPoolBld.setStorPoolName(apiStorPool.getStorPoolName());
        storPoolBld.setStorPoolUuid(apiStorPool.getStorPoolUuid().toString());
        storPoolBld.setNodeName(apiStorPool.getNodeName());
        storPoolBld.setNodeUuid(apiStorPool.getNodeUuid().toString());
        storPoolBld.setStorPoolDfnUuid(apiStorPool.getStorPoolDfnUuid().toString());
        storPoolBld.setDriver(apiStorPool.getDriver());
        storPoolBld.addAllProps(BaseProtoApiCall.fromMap(apiStorPool.getStorPoolProps()));
        storPoolBld.addAllVlms(VlmApiData.toVlmProtoList(apiStorPool.getVlmList()));
        storPoolBld.addAllStaticTraits(BaseProtoApiCall.fromMap(apiStorPool.getStorPoolStaticTraits()));

        return storPoolBld.build();
    }
}
