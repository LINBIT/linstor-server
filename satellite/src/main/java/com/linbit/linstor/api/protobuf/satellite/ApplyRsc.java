package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnFlags;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherNodeNetInterfacePojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtoRscLayerUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntOtherRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.RscConnectionData;
import com.linbit.linstor.stateflags.FlagsHelper;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_RSC,
    description = "Applies resource update data"
)
@Singleton
public class ApplyRsc implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyRsc(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntRscData rscData = MsgIntRscData.parseDelimitedFrom(msgDataIn);

        RscPojo rscRawData = asRscPojo(rscData);
        apiCallHandler.applyResourceChanges(rscRawData);
    }

    //deserialize sync msg and put into pojo, extend rsc api and pojo!
    static RscPojo asRscPojo(MsgIntRscData rscData)
    {
        List<VolumeDefinition.VlmDfnApi> vlmDfns = extractVlmDfns(rscData.getVlmDfnsList());
        List<Volume.VlmApi> localVlms = extractRawVolumes(rscData.getLocalVolumesList());
        List<OtherRscPojo> otherRscList = extractRawOtherRsc(rscData.getOtherResourcesList());
        List<ResourceConnection.RscConnApi> rscConns = extractRscConn(
            rscData.getRscName(),
            rscData.getRscConnectionsList()
        );
        RscDfnPojo rscDfnPojo = new RscDfnPojo(
            UUID.fromString(rscData.getRscDfnUuid()),
            rscData.getRscName(),
            rscData.getRscDfnPort(),
            rscData.getRscDfnSecret(),
            rscData.getRscDfnFlags(),
            rscData.getRscDfnTransportType(),
            rscData.getRscDfnDown(),
            ProtoMapUtils.asMap(rscData.getRscDfnPropsList()),
            vlmDfns
        );
        RscPojo rscRawData = new RscPojo(
            rscData.getRscName(),
            null,
            null,
            rscDfnPojo,
            UUID.fromString(rscData.getLocalRscUuid()),
            rscData.getLocalRscFlags(),
            rscData.getLocalRscNodeId(),
            ProtoMapUtils.asMap(rscData.getLocalRscPropsList()),
            localVlms,
            otherRscList,
            rscConns,
            rscData.getFullSyncId(),
            rscData.getUpdateId(),
            ProtoRscLayerUtils.extractLayerData(rscData.getLayerObject())
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
                    FlagsHelper.fromStringList(VlmDfnFlags.class, vlmDfn.getVlmFlagsList()),
                    ProtoMapUtils.asMap(vlmDfn.getVlmPropsList())
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
                    vol.getBackingDisk(),
                    vol.getMetaDisk(),
                    vol.getDevicePath(),
                    vol.getVlmNr(),
                    vol.getVlmMinorNr(),
                    Volume.VlmFlags.fromStringList(vol.getVlmFlagsList()),
                    ProtoMapUtils.asMap(vol.getVlmPropsList()),
                    vol.getStorPoolDriverName(),
                    UUID.fromString(vol.getStorPoolDfnUuid()),
                    ProtoMapUtils.asMap(vol.getStorPoolDfnPropsList()),
                    ProtoMapUtils.asMap(vol.getStorPoolPropsList()),
                    Optional.empty()
                )
            );
        }
        return list;
    }

    private static List<ResourceConnection.RscConnApi> extractRscConn(
        String rscName,
        List<RscConnectionData> rscConnections
    )
    {
        return rscConnections.stream()
            .map(rscConnData ->
                new RscConnPojo(
                    UUID.fromString(rscConnData.getUuid()),
                    rscConnData.getNode1(),
                    rscConnData.getNode2(),
                    rscName,
                    ProtoMapUtils.asMap(rscConnData.getPropsList()),
                    rscConnData.getFlags(),
                    rscConnData.getPort() == 0 ? null : rscConnData.getPort()
                )
            )
            .collect(Collectors.toList());
    }

    static List<OtherRscPojo> extractRawOtherRsc(List<MsgIntOtherRscData> otherResourcesList)
    {
        List<OtherRscPojo> list = new ArrayList<>();
        for (MsgIntOtherRscData otherRsc : otherResourcesList)
        {
            NodeOuterClass.Node protoNode = otherRsc.getNode();
            list.add(
                new OtherRscPojo(
                    protoNode.getName(),
                    UUID.fromString(protoNode.getUuid()),
                    protoNode.getType(),
                    otherRsc.getNodeFlags(),
                    ProtoMapUtils.asMap(protoNode.getPropsList()),
                    extractNetIfs(protoNode),
                    UUID.fromString(otherRsc.getRscUuid()),
                    otherRsc.getRscNodeId(),
                    otherRsc.getRscFlags(),
                    ProtoMapUtils.asMap(otherRsc.getRscPropsList()),
                    extractRawVolumes(
                        otherRsc.getLocalVlmsList()
                    ),
                    ProtoRscLayerUtils.extractLayerData(otherRsc.getLayerObject())
                )
            );
        }
        return list;
    }

    private static List<OtherNodeNetInterfacePojo> extractNetIfs(NodeOuterClass.Node protoNode)
    {
        List<OtherNodeNetInterfacePojo> list = new ArrayList<>();

        List<NetInterfaceOuterClass.NetInterface> protoNetIfs = protoNode.getNetInterfacesList();

        for (NetInterfaceOuterClass.NetInterface protoNetInterface : protoNetIfs)
        {
            list.add(
                new OtherNodeNetInterfacePojo(
                    UUID.fromString(protoNetInterface.getUuid()),
                    protoNetInterface.getName(),
                    protoNetInterface.getAddress()
                )
            );
        }

        return Collections.unmodifiableList(list);
    }
}
