package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.protobuf.ProtoDeserializationUtils;
import com.linbit.linstor.proto.common.AutoSelectFilterOuterClass.AutoSelectFilter;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.List;

public class AutoSelectFilterApiData implements AutoSelectFilterApi
{
    private AutoSelectFilter selectFilterProto;

    public AutoSelectFilterApiData(AutoSelectFilter selectFilterProtoRef)
    {
        selectFilterProto = selectFilterProtoRef;
    }

    @Override
    public int getPlaceCount()
    {
        return selectFilterProto.getPlaceCount();
    }

    @Override
    public String getStorPoolNameStr()
    {
        String ret = null;
        if (selectFilterProto.hasStoragePool())
        {
            ret = selectFilterProto.getStoragePool();
        }
        return ret;
    }

    @Override
    public List<String> getNotPlaceWithRscList()
    {
        return selectFilterProto.getNotPlaceWithRscList();
    }

    @Override
    public String getNotPlaceWithRscRegex()
    {
        String ret = null;
        if (selectFilterProto.hasNotPlaceWithRscRegex())
        {
            ret = selectFilterProto.getNotPlaceWithRscRegex();
        }
        return ret;
    }

    @Override
    public List<String> getReplicasOnSameList()
    {
        return selectFilterProto.getReplicasOnSameList();
    }

    @Override
    public List<String> getReplicasOnDifferentList()
    {
        return selectFilterProto.getReplicasOnDifferentList();
    }

    @Override
    public List<DeviceLayerKind> getLayerStackList()
    {
        return ProtoDeserializationUtils.parseDeviceLayerKindList(selectFilterProto.getLayerStackList());
    }

    @Override
    public List<DeviceProviderKind> getProviderList()
    {
        return ProtoDeserializationUtils.parseDeviceProviderKind(selectFilterProto.getProvidersList());
    }
}
