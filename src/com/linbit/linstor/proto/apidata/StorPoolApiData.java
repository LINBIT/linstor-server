package com.linbit.linstor.proto.apidata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.StorPoolOuterClass;

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
    public Optional<Long> getFreeSpace()
    {
        Long size = null;
        if (storPool.hasFreeSpace())
        {
            size = storPool.getFreeSpace().getFreeSpace();
        }
        return Optional.ofNullable(size);
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
        return ProtoMapUtils.asMap(storPool.getStaticTraitsList());
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
        storPoolBld.addAllProps(ProtoMapUtils.fromMap(apiStorPool.getStorPoolProps()));
        storPoolBld.addAllVlms(VlmApiData.toVlmProtoList(apiStorPool.getVlmList()));
        storPoolBld.addAllStaticTraits(ProtoMapUtils.fromMap(apiStorPool.getStorPoolStaticTraits()));
        if (apiStorPool.getFreeSpace().isPresent())
        {
            StorPoolFreeSpace.Builder storPoolFreeSpcBld = StorPoolFreeSpace.newBuilder();
            storPoolFreeSpcBld.setStorPoolName(apiStorPool.getStorPoolName())
                .setStorPoolUuid(apiStorPool.getStorPoolUuid().toString())
                .setFreeSpace(apiStorPool.getFreeSpace().get());
            storPoolBld.setFreeSpace(storPoolFreeSpcBld.build());
        }

        return storPoolBld.build();
    }
}
