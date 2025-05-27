package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.interfaces.RscDfnLayerDataApi;
import com.linbit.linstor.api.interfaces.RscLayerDataApi;
import com.linbit.linstor.api.pojo.RscConnPojo;
import com.linbit.linstor.api.pojo.RscDfnPojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherNodeNetInterfacePojo;
import com.linbit.linstor.api.pojo.RscPojo.OtherRscPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmPojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.core.apis.ResourceConnectionApi;
import com.linbit.linstor.core.apis.VolumeApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.proto.common.NetInterfaceOuterClass;
import com.linbit.linstor.proto.common.NodeOuterClass;
import com.linbit.linstor.proto.common.RscConnOuterClass.RscConn;
import com.linbit.linstor.proto.common.RscDfnOuterClass.RscDfn;
import com.linbit.linstor.proto.common.RscOuterClass;
import com.linbit.linstor.proto.common.RscOuterClass.Rsc;
import com.linbit.linstor.proto.common.VlmDfnOuterClass.VlmDfn;
import com.linbit.linstor.proto.common.VlmOuterClass.Vlm;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntOtherRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntRsc;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyRscOuterClass.MsgIntApplyRsc;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.utils.Pair;

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
        MsgIntApplyRsc applyMsg = MsgIntApplyRsc.parseDelimitedFrom(msgDataIn);

        RscPojo rscRawData = asRscPojo(
            applyMsg.getRsc(),
            applyMsg.getFullSyncId(),
            applyMsg.getUpdateId()
        );
        apiCallHandler.applyResourceChanges(rscRawData);
    }

    //deserialize sync msg and put into pojo, extend rsc api and pojo!
    static RscPojo asRscPojo(IntRsc intRscData, long fullSyncId, long updateId)
    {
        Rsc localRsc = intRscData.getLocalRsc();
        RscDfn rscDfn = intRscData.getRscDfn();

        List<VolumeDefinitionApi> vlmDfns = extractVlmDfns(rscDfn.getVlmDfnsList());
        List<VolumeApi> localVlms = extractRawVolumes(localRsc.getVlmsList());
        List<OtherRscPojo> otherRscList = extractRawOtherRsc(
            intRscData.getOtherResourcesList(),
            fullSyncId,
            updateId
        );
        List<ResourceConnectionApi> rscConns = extractRscConn(
            rscDfn.getRscName(),
            intRscData.getRscConnectionsList()
        );

        List<Pair<String, RscDfnLayerDataApi>> layerData = ProtoLayerUtils.extractRscDfnLayerData(
            rscDfn
        );

        RscDfnPojo rscDfnPojo = new RscDfnPojo(
            UUID.fromString(rscDfn.getRscDfnUuid()),
            ProtoDeserializationUtils.parseRscGrp(rscDfn.getRscGrp()),
            rscDfn.getRscName(),
            rscDfn.getExternalName().toByteArray(),
            FlagsHelper.fromStringList(
                ResourceDefinition.Flags.class,
                rscDfn.getRscDfnFlagsList()
            ),
            rscDfn.getRscDfnPropsMap(),
            vlmDfns,
            layerData
        );
        RscLayerDataApi rscLayerData = ProtoLayerUtils.extractRscLayerData(
            localRsc.getLayerObject(),
            fullSyncId,
            updateId
        );
        return new RscPojo(
            localRsc.getName(),
            localRsc.getNodeName(),
            UUID.fromString(localRsc.getNodeUuid()),
            rscDfnPojo,
            UUID.fromString(localRsc.getUuid()),
            FlagsHelper.fromStringList(Resource.Flags.class, localRsc.getRscFlagsList()),
            localRsc.getPropsMap(),
            localVlms,
            otherRscList,
            rscConns,
            fullSyncId,
            updateId,
            rscLayerData,
            null,
            null
        );
    }

    static List<VolumeDefinitionApi> extractVlmDfns(List<VlmDfn> vlmDfnsList)
    {
        List<VolumeDefinitionApi> list = new ArrayList<>();
        for (VlmDfn vlmDfn : vlmDfnsList)
        {
            list.add(
                new VlmDfnPojo(
                    UUID.fromString(vlmDfn.getVlmDfnUuid()),
                    vlmDfn.getVlmNr(),
                    vlmDfn.getVlmSize(),
                    FlagsHelper.fromStringList(VolumeDefinition.Flags.class, vlmDfn.getVlmFlagsList()),
                    vlmDfn.getVlmPropsMap(),
                    ProtoLayerUtils.extractVlmDfnLayerData(vlmDfn.getLayerDataList())
                )
            );
        }
        return list;
    }

    static List<VolumeApi> extractRawVolumes(List<Vlm> localVolumesList)
    {
        List<VolumeApi> list = new ArrayList<>();
        for (Vlm vol : localVolumesList)
        {
            list.add(
                new VlmPojo(
                    UUID.fromString(vol.getVlmDfnUuid()),
                    UUID.fromString(vol.getVlmUuid()),
                    vol.getDevicePath(),
                    vol.getVlmNr(),
                    Volume.Flags.fromStringList(vol.getVlmFlagsList()),
                    vol.getVlmPropsMap(),
                    Optional.empty(),
                    Optional.empty(),

                    // protobuf does not support circular imports, which is what we are creating here
                    // therefore, we skip this data. satellite should get the layerData from rsc-level.
                    Collections.emptyList(),
                    null, // no need for compat on stlt
                    null, // no need for compat on stlt
                    new ApiCallRcImpl()
                )
            );
        }
        return list;
    }

    private static List<ResourceConnectionApi> extractRscConn(
        String rscName,
        List<RscConn> rscConnections
    )
    {
        return rscConnections.stream()
            .map(
                rscConnData -> new RscConnPojo(
                    UUID.fromString(rscConnData.getRscConnUuid()),
                    rscConnData.getNodeName1(),
                    rscConnData.getNodeName2(),
                    rscName,
                    rscConnData.getRscConnPropsMap(),
                    FlagsHelper.fromStringList(
                        ResourceConnection.Flags.class,
                        rscConnData.getRscConnFlagsList()
                    ),
                    rscConnData.getDrbdProxyPortSource() == 0 ? null : rscConnData.getDrbdProxyPortSource(),
                    rscConnData.getDrbdProxyPortTarget() == 0 ? null : rscConnData.getDrbdProxyPortTarget()
                )
            )
            .collect(Collectors.toList());
    }

    static List<OtherRscPojo> extractRawOtherRsc(
        List<IntOtherRsc> otherResourcesList,
        long fullSyncId,
        long updateId
    )
    {
        List<OtherRscPojo> list = new ArrayList<>();
        for (IntOtherRsc intOtherRsc : otherResourcesList)
        {
            NodeOuterClass.Node protoNode = intOtherRsc.getNode();
            RscOuterClass.Rsc protoRsc = intOtherRsc.getRsc();

            list.add(
                new OtherRscPojo(
                    protoNode.getName(),
                    UUID.fromString(protoNode.getUuid()),
                    protoNode.getType(),
                    FlagsHelper.fromStringList(Node.Flags.class, protoNode.getFlagsList()),
                    protoNode.getPropsMap(),
                    extractNetIfs(protoNode),
                    UUID.fromString(protoRsc.getUuid()),
                    FlagsHelper.fromStringList(Resource.Flags.class, protoRsc.getRscFlagsList()),
                    protoRsc.getPropsMap(),
                    extractRawVolumes(
                        protoRsc.getVlmsList()
                    ),
                    ProtoLayerUtils.extractRscLayerData(
                        protoRsc.getLayerObject(),
                        fullSyncId,
                        updateId
                    )
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
