package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.core.apis.StorPoolApi;
import com.linbit.linstor.proto.common.StorPoolOuterClass;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 *
 * @author rpeinthor
 */
public class StorPoolApiData implements StorPoolApi
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
            kind = ProtoDeserializationUtils.parseDeviceProviderKind(storPool.getProviderKind());
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
        return storPool.getPropsMap();
    }

    @Override
    public Map<String, String> getStorPoolStaticTraits()
    {
        return storPool.getStaticTraitsMap();
    }

    @Override
    public ApiCallRc getReports()
    {
        return new ApiCallRcImpl();
    }

    @Override
    public Map<String, String> getStorPoolDfnProps()
    {
        return storPool.getStorPoolDfnProps();
    }

    @Override
    public Boolean supportsSnapshots()
    {
        return storPool.getSnapshotSupported();
    }

    @Override
    public Boolean isPmem()
    {
        return storPool.getIsPmem();
    }
}
