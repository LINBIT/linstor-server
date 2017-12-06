package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.api.pojo.RscPojo.VolumeDfnPojo;
import com.linbit.linstor.api.pojo.RscPojo.VolumePojo;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.security.AccessContext;
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

        RscPojo rscRawData = asRscPojo(rscData);
        satellite.getApiCallHandler().deployResource(rscRawData);
    }

    static RscPojo asRscPojo(MsgIntRscData rscData)
    {
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
        return rscRawData;
    }

    static List<VolumeDfnPojo> extractVlmDfns(List<VlmDfn> vlmDfnsList)
    {
        List<VolumeDfnPojo> list = new ArrayList<>();
        for (VlmDfn vlmDfn : vlmDfnsList)
        {
            list.add(
                new VolumeDfnPojo(
                    UuidUtils.asUuid(vlmDfn.getVlmDfnUuid().toByteArray()),
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

    static List<VolumePojo> extractRawVolumes(List<Vlm> localVolumesList)
    {
        List<VolumePojo> list = new ArrayList<>();
        for (Vlm vol : localVolumesList)
        {
            list.add(
                new VolumePojo(
                    UuidUtils.asUuid(vol.getVlmUuid().toByteArray()),
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

    static List<OtherRscPojo> extractRawOtherRsc(List<MsgIntOtherRscData> otherResourcesList)
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
