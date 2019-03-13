package com.linbit.linstor.proto.apidata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.linbit.linstor.StorPool;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.common.LinStorMapEntryOuterClass;
import com.linbit.linstor.proto.common.StorPoolOuterClass;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

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
    public DeviceProviderKind getDeviceProviderKind()
    {
        DeviceProviderKind kind = null;
        if (storPool.hasProviderKind())
        {
            kind = ProtoDeserializationUtils.parseProviderKind(storPool.getProviderKind());
        }
        return kind;
    }

    @Override
    public String getFreeSpaceManagerName()
    {
        return storPool.getFreeSpaceMgrName();
    }

    @Override
    public Optional<Long> getFreeCapacity()
    {
        Long size = null;
        if (storPool.hasFreeSpace())
        {
            size = storPool.getFreeSpace().getFreeCapacity();
        }
        return Optional.ofNullable(size);
    }

    @Override
    public Optional<Long> getTotalCapacity()
    {
        Long size = null;
        if (storPool.hasFreeSpace())
        {
            size = storPool.getFreeSpace().getTotalCapacity();
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
}
