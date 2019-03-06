package com.linbit.linstor.proto.apidata;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Resource.RscApi;
import com.linbit.linstor.api.protobuf.ProtoLayerUtils;
import com.linbit.linstor.proto.requests.MsgCrtRscOuterClass.RscWithPayload;

import java.util.List;
import java.util.stream.Collectors;

public class RscWithPayloadApiData implements Resource.RscWithPayloadApi
{
    private RscApiData rscApi;
    private Integer drbdNodeId;
    private List<String> layerStackList;

    public RscWithPayloadApiData(RscWithPayload rscWithPayloadRef)
    {
        rscApi = new RscApiData(rscWithPayloadRef.getRsc());
        drbdNodeId = rscWithPayloadRef.hasDrbdNodeId() ? rscWithPayloadRef.getDrbdNodeId() : null;
        layerStackList = rscWithPayloadRef.getLayerStackList().stream()
            .map(ProtoLayerUtils::layerType2layerString).collect(Collectors.toList());
    }

    @Override
    public RscApi getRscApi()
    {
        return rscApi;
    }

    @Override
    public List<String> getLayerStack()
    {
        return layerStackList;
    }

    @Override
    public Integer getDrbdNodeId()
    {
        return drbdNodeId;
    }

}
