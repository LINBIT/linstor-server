package com.linbit.drbdmanage.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.drbdmanage.InternalApiConsts;
import com.linbit.drbdmanage.api.pojo.RscPojo;
import com.linbit.drbdmanage.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.drbdmanage.api.pojo.RscPojo.VolumeDfnPojo;
import com.linbit.drbdmanage.api.pojo.RscPojo.VolumePojo;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
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
public class ApplyRsc extends BaseProtoApiCall
{
    private Satellite satellite;

    public ApplyRsc(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_RSC;
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

        List<VolumeDfnPojo> vlmDfns = extractVlmDfns(rscData.getVlmDfnsList());
        List<VolumePojo> localVlms = extractRawVolumes(rscData.getLocalVolumesList());
        List<OtherRscPojo> otherRscList = extractRawOtherRsc(rscData.getOtherResourcesList());
        RscPojo rscRawData = new RscPojo(
            rscData.getRscName(),
            UuidUtils.asUuid(rscData.getRscDfnUuid().toByteArray()),
            rscData.getRscDfnPort(),
            rscData.getRscDfnFlags(),
            rscData.getRscDfnSecret(),
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

    private List<VolumeDfnPojo> extractVlmDfns(List<VlmDfn> vlmDfnsList)
    {
        List<VolumeDfnPojo> list = new ArrayList<>();
        for (VlmDfn vlmDfn : vlmDfnsList)
        {
            list.add(
                new VolumeDfnPojo(
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

    private List<VolumePojo> extractRawVolumes(List<Vlm> localVolumesList)
    {
        List<VolumePojo> list = new ArrayList<>();
        for (Vlm vol : localVolumesList)
        {
            list.add(
                new VolumePojo(
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

    private List<OtherRscPojo> extractRawOtherRsc(List<MsgIntOtherRscData> otherResourcesList)
    {
        List<OtherRscPojo> list = new ArrayList<>();
        for (MsgIntOtherRscData otherRsc : otherResourcesList)
        {
            list.add(
                new OtherRscPojo(
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
