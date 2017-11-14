package com.linbit.drbdmanage.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.api.raw.ResourceRawData;
import com.linbit.drbdmanage.api.raw.ResourceRawData.OtherRscRawData;
import com.linbit.drbdmanage.api.raw.ResourceRawData.VolumeDfnRawData;
import com.linbit.drbdmanage.api.raw.ResourceRawData.VolumeRawData;
import com.linbit.drbdmanage.core.Satellite;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgCrtRscOuterClass.Vlm;
import com.linbit.drbdmanage.proto.MsgCrtVlmDfnOuterClass.VlmDfn;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.drbdmanage.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class RscData extends BaseProtoApiCall
{
    private Satellite satellite;

    public RscData(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_RSC_DATA;
    }

    @Override
    public String getDescription()
    {
        return "Deployes the resources";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntRscData rscData = MsgIntRscData.parseDelimitedFrom(msgDataIn);

        List<VolumeDfnRawData> vlmDfns = extractVlmDfns(rscData.getVlmDfnsList());
        List<VolumeRawData> localVlms = extractRawVolumes(rscData.getLocalVolumesList());
        List<OtherRscRawData> otherRscList = extractRawOtherRsc(rscData.getOtherResourcesList());
        ResourceRawData rscRawData = new ResourceRawData(
            rscData.getRscName(),
            UuidUtils.asUuid(rscData.getRscDfnUuid().toByteArray()),
            rscData.getRscDfnPort(),
            rscData.getRscDfnFlags(),
            asMap(rscData.getRscDfnPropsList()),
            UuidUtils.asUuid(rscData.getLocalRscUuid().toByteArray()),
            rscData.getLocalRscFlags(),
            rscData.getLocalRscNodeId(),
            asMap(rscData.getLocalRscPropsList()),
            vlmDfns,
            localVlms,
            otherRscList
        );
        satellite.getApiCallHandler().deployResource(rscRawData);
    }

    private List<VolumeDfnRawData> extractVlmDfns(List<VlmDfn> vlmDfnsList)
    {
        List<VolumeDfnRawData> list = new ArrayList<>();
        for (VlmDfn vlmDfn : vlmDfnsList)
        {
            list.add(
                new VolumeDfnRawData(
                    UuidUtils.asUuid(vlmDfn.getUuid().toByteArray()),
                    vlmDfn.getVlmNr(),
                    vlmDfn.getVlmSize(),
                    vlmDfn.getVlmMinor(),
                    vlmDfn.getVlmFlags(),
                    asMap(vlmDfn.getVlmPropsList())
                )
            );
        }
        return list;
    }

    private List<VolumeRawData> extractRawVolumes(List<Vlm> localVolumesList)
    {
        List<VolumeRawData> list = new ArrayList<>();
        for (Vlm vol : localVolumesList)
        {
            list.add(
                new VolumeRawData(
                    UuidUtils.asUuid(vol.getUuid().toByteArray()),
                    vol.getVlmNr(),
                    vol.getBlockDevice(),
                    vol.getMetaDisk(),
                    vol.getVlmFlags(),
                    UuidUtils.asUuid(vol.getStorPoolUuid().toByteArray()),
                    vol.getStorPoolName(),
                    asMap(vol.getVlmPropsList())
                )
            );
        }
        return list;
    }

    private List<OtherRscRawData> extractRawOtherRsc(List<MsgIntOtherRscData> otherResourcesList)
    {
        List<OtherRscRawData> list = new ArrayList<>();
        for (MsgIntOtherRscData otherRsc : otherResourcesList)
        {
            list.add(
                new OtherRscRawData(
                    otherRsc.getNodeName(),
                    UuidUtils.asUuid(otherRsc.getNodeUuid().toByteArray()),
                    otherRsc.getNodeType(),
                    otherRsc.getNodeFlags(),
                    asMap(otherRsc.getNodePropsList()),
                    UuidUtils.asUuid(otherRsc.getRscUuid().toByteArray()),
                    otherRsc.getRscNodeId(),
                    otherRsc.getRscFlags(),
                    asMap(otherRsc.getRscPropsList()),
                    extractRawVolumes(
                        otherRsc.getLocalVlmsList()
                    )
                )
            );
        }
        return list;
    }
}
