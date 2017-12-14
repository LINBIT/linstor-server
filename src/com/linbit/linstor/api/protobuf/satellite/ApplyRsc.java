package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmPojo;
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
import java.util.UUID;

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
        List<VolumeDefinition.VlmDfnApi> vlmDfns = extractVlmDfns(rscData.getVlmDfnsList());
        List<Volume.VlmApi> localVlms = extractRawVolumes(rscData.getLocalVolumesList());
        List<OtherRscPojo> otherRscList = extractRawOtherRsc(rscData.getOtherResourcesList());
        RscDfnPojo rscDfnPojo = new RscDfnPojo(
                UUID.fromString(rscData.getRscDfnUuid()),
                rscData.getRscName(),
                rscData.getRscDfnPort(),
                rscData.getRscDfnSecret(),
                rscData.getRscDfnFlags(),
                rscData.getRscDfnTransportType(),
                asMap(rscData.getRscDfnPropsList()),
                vlmDfns);
        RscPojo rscRawData = new RscPojo(
            rscData.getRscName(),
            null,
            null,
            rscDfnPojo,
            UUID.fromString(rscData.getLocalRscUuid()),
            rscData.getLocalRscFlags(),
            rscData.getLocalRscNodeId(),
            asMap(rscData.getLocalRscPropsList()),
            localVlms,
            otherRscList
        );
        return rscRawData;
    }

    static List<VolumeDefinition.VlmDfnApi> extractVlmDfns(List<VlmDfn> vlmDfnsList)
    {
        List<VolumeDefinition.VlmDfnApi> list = new ArrayList<>();
        for (VlmDfn vlmDfn : vlmDfnsList)
        {
            list.add(
                new VlmDfnPojo(
                    UUID.fromString(vlmDfn.getVlmDfnUuid()),
                    vlmDfn.getVlmNr(),
                    vlmDfn.getVlmMinor(),
                    vlmDfn.getVlmSize(),
                    vlmDfn.getVlmFlags(),
                    asMap(vlmDfn.getVlmPropsList())
                )
            );
        }
        return list;
    }

    static List<Volume.VlmApi> extractRawVolumes(List<Vlm> localVolumesList)
    {
        List<Volume.VlmApi> list = new ArrayList<>();
        for (Vlm vol : localVolumesList)
        {
            list.add(
                new VlmPojo(
                    vol.getStorPoolName(),
                    UUID.fromString(vol.getStorPoolUuid()),
                    UUID.fromString(vol.getVlmDfnUuid()),
                    UUID.fromString(vol.getVlmUuid()),
                    vol.getBlockDevice(),
                    vol.getMetaDisk(),
                    vol.getVlmNr(),
                    vol.getVlmFlags(),
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
                    UUID.fromString(otherRsc.getNodeUuid()),
                    otherRsc.getNodeType(),
                    otherRsc.getNodeFlags(),
                    asMap(otherRsc.getNodePropsList()),
                    UUID.fromString(otherRsc.getRscUuid()),
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
