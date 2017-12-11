/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.linbit.linstor.proto.apidata;

import com.google.protobuf.ByteString;
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
public class StorPoolApiData implements StorPool.StorPoolApi {
    private StorPoolOuterClass.StorPool storPool;

    public StorPoolApiData(StorPoolOuterClass.StorPool refStorPool)
    {
        storPool = refStorPool;
    }

    @Override
    public UUID getStorPoolUuid() {
        UUID uuid = null;
        if (storPool.hasStorPoolUuid())
        {
            uuid = UUID.nameUUIDFromBytes(storPool.getStorPoolUuid().toByteArray());
        }
        return uuid;
    }

    @Override
    public String getStorPoolName() {
        return storPool.getStorPoolName();
    }

    @Override
    public String getNodeName() {
        return storPool.getNodeName();
    }

    @Override
    public UUID getNodeUuid() {
        UUID uuid = null;
        if (storPool.hasNodeUuid())
            uuid = UUID.nameUUIDFromBytes(storPool.getNodeUuid().toByteArray());
        return uuid;
    }

    @Override
    public UUID getStorPoolDfnUuid() {
        UUID uuid = null;
        if (storPool.hasStorPoolDfnUuid())
            uuid = UUID.nameUUIDFromBytes(storPool.getStorPoolDfnUuid().toByteArray());
        return uuid;
    }

    @Override
    public String getDriver() {
        return storPool.getDriver();
    }

    @Override
    public Map<String, String> getStorPoolProps() {
        Map<String, String> ret = new HashMap<>();
        for (LinStorMapEntryOuterClass.LinStorMapEntry entry : storPool.getPropsList())
        {
            ret.put(entry.getKey(), entry.getValue());
        }
        return ret;
    }

    @Override
    public List<Volume.VlmApi> getVlmList() {
        return VlmApiData.toApiList(storPool.getVlmsList());
    }

    public static StorPoolOuterClass.StorPool toStorPoolProto(final StorPool.StorPoolApi apiStorPool)
    {
        StorPoolOuterClass.StorPool.Builder storPoolBld = StorPoolOuterClass.StorPool.newBuilder();
        storPoolBld.setStorPoolName(apiStorPool.getStorPoolName());
        storPoolBld.setStorPoolUuid(ByteString.copyFrom(apiStorPool.getStorPoolUuid().toString().getBytes()));
        storPoolBld.setNodeName(apiStorPool.getNodeName());
        storPoolBld.setNodeUuid(ByteString.copyFrom(apiStorPool.getNodeUuid().toString().getBytes()));
        storPoolBld.setStorPoolDfnUuid(ByteString.copyFrom(
                apiStorPool.getStorPoolDfnUuid().toString().getBytes()));
        storPoolBld.setDriver(apiStorPool.getDriver());
        storPoolBld.addAllProps(BaseProtoApiCall.fromMap(apiStorPool.getStorPoolProps()));
        storPoolBld.addAllVlms(VlmApiData.toVlmProtoList(apiStorPool.getVlmList()));

        return storPoolBld.build();
    }

}
