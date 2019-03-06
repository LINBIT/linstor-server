package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Volume;
import java.util.Map;

import com.linbit.linstor.Volume.VlmApi;
import com.linbit.linstor.api.interfaces.VlmLayerDataApi;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.proto.common.LinStorMapEntryOuterClass.LinStorMapEntry;
import com.linbit.linstor.proto.common.VlmOuterClass.Vlm;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class VlmApiData implements VlmApi
{
    private final Vlm vlm;
    private final List<Pair<String, VlmLayerDataApi>> layerData;

    public VlmApiData(Vlm vlmRef)
    {
        vlm = vlmRef;
        layerData = ProtoLayerUtils.extractVlmLayerData(vlmRef.getLayerDataList());
    }

    @Override
    public UUID getVlmUuid()
    {
        return vlm.hasVlmUuid() ? UUID.fromString(vlm.getVlmUuid()) : null;
    }

    @Override
    public UUID getVlmDfnUuid()
    {
        return vlm.hasVlmDfnUuid() ? UUID.fromString(vlm.getVlmDfnUuid()) : null;
    }

    @Override
    public String getStorPoolName()
    {
        return vlm.getStorPoolName();
    }

    @Override
    public UUID getStorPoolUuid()
    {
        return vlm.hasStorPoolUuid() ? UUID.fromString(vlm.getStorPoolUuid()) : null;
    }

    @Override
    public String getDevicePath()
    {
        return vlm.getDevicePath();
    }

    @Override
    public int getVlmNr()
    {
        return vlm.getVlmNr();
    }

    @Override
    public long getFlags()
    {
        return Volume.VlmFlags.fromStringList(vlm.getVlmFlagsList());
    }

    @Override
    public String getStorDriverSimpleClassName()
    {
        String storPoolDriverName = null;
        if (vlm.hasStorPoolDriverName())
        {
            storPoolDriverName = vlm.getStorPoolDriverName();
        }
        return storPoolDriverName;
    }

    @Override
    public UUID getStorPoolDfnUuid()
    {
        UUID storPoolDfnUuid = null;
        if (vlm.hasStorPoolDfnUuid())
        {
            storPoolDfnUuid = UUID.fromString(vlm.getVlmDfnUuid());
        }
        return storPoolDfnUuid;
    }

    @Override
    public Map<String, String> getVlmProps()
    {
        return vlm.getVlmPropsList().stream()
            .collect(Collectors.toMap(LinStorMapEntry::getKey, LinStorMapEntry::getValue));
    }

    @Override
    public Map<String, String> getStorPoolDfnProps()
    {
        return vlm.getStorPoolDfnPropsList().stream()
            .collect(Collectors.toMap(LinStorMapEntry::getKey, LinStorMapEntry::getValue));
    }

    @Override
    public Map<String, String> getStorPoolProps()
    {
        return vlm.getStorPoolPropsList().stream()
            .collect(Collectors.toMap(LinStorMapEntry::getKey, LinStorMapEntry::getValue));
    }

    @Override
    public Optional<Long> getAllocatedSize()
    {
        return vlm.hasAllocatedSize() ?
            Optional.of(vlm.getAllocatedSize()) :
            Optional.empty();
    }

    @Override
    public Optional<Long> getUsableSize()
    {
        return vlm.hasUsableSize() ?
            Optional.of(vlm.getUsableSize()) :
            Optional.empty();
    }

    @Override
    public List<Pair<String, VlmLayerDataApi>> getVlmLayerData()
    {
        return layerData;
    }

    public static Vlm toVlmProto(final VlmApi vlmApi)
    {
        Vlm.Builder builder = Vlm.newBuilder();
        builder.setVlmUuid(vlmApi.getVlmUuid().toString());
        builder.setVlmDfnUuid(vlmApi.getVlmDfnUuid().toString());
        builder.setStorPoolName(vlmApi.getStorPoolName());
        builder.setStorPoolUuid(vlmApi.getStorPoolUuid().toString());
        builder.setVlmNr(vlmApi.getVlmNr());
        if (vlmApi.getDevicePath() != null)
        {
            builder.setDevicePath(vlmApi.getDevicePath());
        }
        builder.addAllVlmFlags(Volume.VlmFlags.toStringList(vlmApi.getFlags()));
        builder.addAllVlmProps(ProtoMapUtils.fromMap(vlmApi.getVlmProps()));
        vlmApi.getAllocatedSize().ifPresent(builder::setAllocatedSize);

        return builder.build();
    }

    public static List<Vlm> toVlmProtoList(final List<? extends VlmApi> volumedefs)
    {
        ArrayList<Vlm> protoVlm = new ArrayList<>();
        for (VlmApi vlmapi : volumedefs)
        {
            protoVlm.add(VlmApiData.toVlmProto(vlmapi));
        }
        return protoVlm;
    }

    public static List<VlmApi> toApiList(final List<Vlm> volumedefs)
    {
        ArrayList<VlmApi> apiVlms = new ArrayList<>();
        for (Vlm vlm : volumedefs)
        {
            apiVlms.add(new VlmApiData(vlm));
        }
        return apiVlms;
    }
}
