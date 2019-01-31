package com.linbit.linstor.api.protobuf.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.linbit.linstor.KeyValueStore;
import com.linbit.linstor.Node;
import com.linbit.linstor.Node.NodeApi;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource.RscApi;
import com.linbit.linstor.ResourceConnection;
import com.linbit.linstor.ResourceDefinition.RscDfnApi;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.StorPool.StorPoolApi;
import com.linbit.linstor.StorPoolDefinition.StorPoolDfnApi;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer.CtrlClientSerializerBuilder;
import com.linbit.linstor.api.protobuf.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityAutoPoolSelectorUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.KvsOuterClass;
import com.linbit.linstor.proto.MsgApiVersionOuterClass.MsgApiVersion;
import com.linbit.linstor.proto.MsgLstCtrlCfgPropsOuterClass.MsgLstCtrlCfgProps;
import com.linbit.linstor.proto.MsgLstNodeOuterClass;
import com.linbit.linstor.proto.MsgLstRscDfnOuterClass.MsgLstRscDfn;
import com.linbit.linstor.proto.MsgRspKvsOuterClass;
import com.linbit.linstor.proto.MsgRspMaxVlmSizesOuterClass;
import com.linbit.linstor.proto.MsgRspMaxVlmSizesOuterClass.MsgRspMaxVlmSizes;
import com.linbit.linstor.proto.MsgLstRscOuterClass;
import com.linbit.linstor.proto.MsgLstSnapshotDfnOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolDfnOuterClass;
import com.linbit.linstor.proto.MsgLstStorPoolOuterClass;
import com.linbit.linstor.proto.MsgLstRscConnOuterClass.MsgLstRscConn;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.RscStateOuterClass;
import com.linbit.linstor.proto.SnapshotDfnOuterClass;
import com.linbit.linstor.proto.VlmStateOuterClass;
import com.linbit.linstor.proto.apidata.NetInterfaceApiData;
import com.linbit.linstor.proto.apidata.RscApiData;
import com.linbit.linstor.proto.apidata.RscConnApiData;
import com.linbit.linstor.proto.apidata.RscDfnApiData;
import com.linbit.linstor.proto.apidata.StorPoolApiData;
import com.linbit.linstor.proto.apidata.StorPoolDfnApiData;
import com.linbit.linstor.satellitestate.SatelliteResourceState;
import com.linbit.linstor.satellitestate.SatelliteState;
import com.linbit.linstor.satellitestate.SatelliteVolumeState;
import com.linbit.linstor.security.AccessContext;

public class ProtoCtrlClientSerializerBuilder
    extends ProtoCommonSerializerBuilder implements CtrlClientSerializer.CtrlClientSerializerBuilder
{
    public ProtoCtrlClientSerializerBuilder(
        final ErrorReporter errReporterRef,
        final AccessContext serializerCtxRef,
        final String apiCall,
        Long apiCallId,
        boolean isAnswer
    )
    {
        super(errReporterRef, serializerCtxRef, apiCall, apiCallId, isAnswer);
    }

    @Override
    public ProtoCtrlClientSerializerBuilder nodeList(List<NodeApi> nodes)
    {
        try
        {
            MsgLstNodeOuterClass.MsgLstNode.Builder msgListNodeBuilder = MsgLstNodeOuterClass.MsgLstNode.newBuilder();

            for (NodeApi nodeApi : nodes)
            {
                NodeOuterClass.Node.Builder bld = NodeOuterClass.Node.newBuilder();
                bld.setName(nodeApi.getName());
                bld.setType(nodeApi.getType());
                bld.setUuid(nodeApi.getUuid().toString());
                bld.addAllProps(ProtoMapUtils.fromMap(nodeApi.getProps()));
                bld.addAllFlags(Node.NodeFlag.toStringList(nodeApi.getFlags()));
                bld.addAllNetInterfaces(NetInterfaceApiData.toNetInterfaceProtoList(nodeApi.getNetInterfaces()));
                bld.setConnectionStatus(NodeOuterClass.Node.ConnectionStatus.forNumber(nodeApi.connectionStatus().value()));

                msgListNodeBuilder.addNodes(bld.build());
            }

            msgListNodeBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder storPoolDfnList(List<StorPoolDfnApi> storPoolDfns)
    {
        try
        {
            MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.Builder msgListStorPoolDfnsBuilder =
                MsgLstStorPoolDfnOuterClass.MsgLstStorPoolDfn.newBuilder();

            for (StorPoolDfnApi apiDfn: storPoolDfns)
            {
                msgListStorPoolDfnsBuilder.addStorPoolDfns(StorPoolDfnApiData.fromStorPoolDfnApi(apiDfn));
            }

            msgListStorPoolDfnsBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder storPoolList(List<StorPoolApi> storPools)
    {
        try
        {
            MsgLstStorPoolOuterClass.MsgLstStorPool.Builder msgListBuilder =
                MsgLstStorPoolOuterClass.MsgLstStorPool.newBuilder();
            for (StorPoolApi apiStorPool: storPools)
            {
                msgListBuilder.addStorPools(StorPoolApiData.toStorPoolProto(apiStorPool));
            }

            msgListBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder resourceDfnList(List<RscDfnApi> rscDfns)
    {
        try
        {
            MsgLstRscDfn.Builder msgListRscDfnsBuilder = MsgLstRscDfn.newBuilder();

            for (RscDfnApi apiRscDfn: rscDfns)
            {
                msgListRscDfnsBuilder.addRscDfns(RscDfnApiData.fromRscDfnApi(apiRscDfn));
            }

            msgListRscDfnsBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder resourceList(
        List<RscApi> rscs,
        Map<NodeName, SatelliteState> satelliteStates
    )
    {
        try
        {
            MsgLstRscOuterClass.MsgLstRsc.Builder msgListRscsBuilder = MsgLstRscOuterClass.MsgLstRsc.newBuilder();

            for (RscApi apiRsc: rscs)
            {
                msgListRscsBuilder.addResources(RscApiData.toRscProto(apiRsc));
            }

            for (Map.Entry<NodeName, SatelliteState> satelliteEntry : satelliteStates.entrySet())
            {
                for (Map.Entry<ResourceName, SatelliteResourceState> resourceEntry :
                    satelliteEntry.getValue().getResourceStates().entrySet())
                {
                    msgListRscsBuilder.addResourceStates(
                        buildResourceState(
                            satelliteEntry.getKey(),
                            resourceEntry.getKey(),
                            resourceEntry.getValue()
                        )
                    );
                }
            }

            msgListRscsBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder resourceConnList(List<ResourceConnection.RscConnApi> rscConns)
    {
        try
        {
            MsgLstRscConn.Builder msgListRscConnBuilder = MsgLstRscConn.newBuilder();

            msgListRscConnBuilder.addAllRscConnections(
                rscConns.stream()
                    .map(RscConnApiData::toProto).collect(Collectors.toList())
            );

            msgListRscConnBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder snapshotDfnList(
        List<SnapshotDefinition.SnapshotDfnListItemApi> snapshotDfns
    )
    {
        try
        {
            MsgLstSnapshotDfnOuterClass.MsgLstSnapshotDfn.Builder msgListSnapshotBuilder =
                MsgLstSnapshotDfnOuterClass.MsgLstSnapshotDfn.newBuilder();

            for (SnapshotDefinition.SnapshotDfnListItemApi snapshotDfnApi : snapshotDfns)
            {
                SnapshotDfnOuterClass.SnapshotDfn.Builder snapshotDfnBuilder =
                    SnapshotDfnOuterClass.SnapshotDfn.newBuilder();

                snapshotDfnBuilder
                    .setUuid(snapshotDfnApi.getUuid().toString())
                    .setSnapshotName(snapshotDfnApi.getSnapshotName())
                    .setRscDfnUuid(snapshotDfnApi.getUuid().toString())
                    .setRscName(snapshotDfnApi.getRscDfn().getResourceName())
                    .addAllSnapshotDfnFlags(SnapshotDefinition.SnapshotDfnFlags.toStringList(snapshotDfnApi.getFlags()))
                    .addAllSnapshots(snapshotDfnApi.getNodeNames().stream()
                        .map(nodeName -> SnapshotDfnOuterClass.Snapshot.newBuilder().setNodeName(nodeName).build())
                        .collect(Collectors.toList()))
                    .addAllSnapshotVlmDfns(snapshotDfnApi.getSnapshotVlmDfnList().stream()
                        .map(snapshotVlmDfnApi -> SnapshotDfnOuterClass.SnapshotVlmDfn.newBuilder()
                            .setVlmNr(snapshotVlmDfnApi.getVolumeNr())
                            .setVlmSize(snapshotVlmDfnApi.getSize())
                            .build())
                        .collect(Collectors.toList()));

                msgListSnapshotBuilder.addSnapshotDfns(snapshotDfnBuilder.build());
            }

            msgListSnapshotBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder apiVersion(
        final long features,
        final String controllerInfo
    )
    {
        try
        {
            MsgApiVersion.Builder msgApiVersion = MsgApiVersion.newBuilder();

            // set features
            msgApiVersion.setVersion(Controller.API_VERSION);
            msgApiVersion.setMinVersion(Controller.API_MIN_VERSION);
            msgApiVersion.setFeatures(features);
            msgApiVersion.setControllerInfo(controllerInfo);
            msgApiVersion.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public ProtoCtrlClientSerializerBuilder ctrlCfgSingleProp(String namespace, String key, String value)
    {
        Map<String, String> map = new HashMap<>();
        map.put(getFullKey(namespace, key), value);
        return ctrlCfgProps(map);
    }

    @Override
    public ProtoCtrlClientSerializerBuilder ctrlCfgProps(Map<String, String> map)
    {
        try
        {
            MsgLstCtrlCfgProps.newBuilder()
                .addAllProps(
                    ProtoMapUtils.fromMap(map)
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder maxVlmSizeCandidateList(List<MaxVlmSizeCandidatePojo> candidates)
    {
        try
        {
            List<MsgRspMaxVlmSizesOuterClass.Candidate> protoCandidates = new ArrayList<>();

            for (MaxVlmSizeCandidatePojo maxVlmSizeCandidatePojo : candidates)
            {
                protoCandidates.add(
                    MsgRspMaxVlmSizesOuterClass.Candidate.newBuilder()
                        .setMaxVlmSize(maxVlmSizeCandidatePojo.getMaxVlmSize())
                        .addAllNodeNames(maxVlmSizeCandidatePojo.getNodeNames())
                        .setStorPoolDfn(StorPoolDfnApiData.fromStorPoolDfnApi(
                            maxVlmSizeCandidatePojo.getStorPoolDfnApi()
                        ))
                        .setAllThin(maxVlmSizeCandidatePojo.areAllThin())
                        .build()
                );
            }

            MsgRspMaxVlmSizes.newBuilder()
                .addAllCandidates(protoCandidates)
                .setDefaultMaxOversubscriptionRatio(
                    Double.toString(FreeCapacityAutoPoolSelectorUtils.DEFAULT_MAX_OVERSUBSCRIPTION_RATIO)
                )
                .build()
                .writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    @Override
    public CtrlClientSerializerBuilder keyValueStoreList(Set<KeyValueStore.KvsApi> kvsSet)
    {
        try
        {
            MsgRspKvsOuterClass.MsgRspKvs.Builder msgListKvsBuilder = MsgRspKvsOuterClass.MsgRspKvs.newBuilder();

            for (KeyValueStore.KvsApi kvsApi : kvsSet)
            {
                msgListKvsBuilder.addKeyValueStore(
                    KvsOuterClass.Kvs.newBuilder()
                        .setName(kvsApi.getName())
                        .addAllProps(ProtoMapUtils.fromMap(kvsApi.getProps()))
                );
            }

            msgListKvsBuilder.build().writeDelimitedTo(baos);
        }
        catch (IOException exc)
        {
            handleIOException(exc);
        }
        return this;
    }

    private String getFullKey(String namespace, String key)
    {
        String fullKey;
        if (namespace != null && !namespace.trim().equals(""))
        {
            fullKey = namespace + "/" + key;
        }
        else
        {
            fullKey = key;
        }
        return fullKey;
    }

    private static VlmStateOuterClass.VlmState buildVolumeState(
        VolumeNumber volumeNumber, SatelliteVolumeState volumeState)
    {
        VlmStateOuterClass.VlmState.Builder vlmStateBuilder = VlmStateOuterClass.VlmState.newBuilder();

        vlmStateBuilder
            .setVlmNr(volumeNumber.value);

        if (volumeState.getDiskState() != null)
        {
            vlmStateBuilder.setDiskState(volumeState.getDiskState());
        }

        return vlmStateBuilder.build();
    }

    private static RscStateOuterClass.RscState buildResourceState(
        final NodeName nodeName,
        final ResourceName resourceName,
        final SatelliteResourceState resourceState
    )
    {
        RscStateOuterClass.RscState.Builder rscStateBuilder = RscStateOuterClass.RscState.newBuilder();

        rscStateBuilder
            .setNodeName(nodeName.displayValue)
            .setRscName(resourceName.displayValue);

        if (resourceState.isInUse() != null)
        {
            rscStateBuilder.setInUse(resourceState.isInUse());
        }

        // volumes
        for (Map.Entry<VolumeNumber, SatelliteVolumeState> volumeEntry : resourceState.getVolumeStates().entrySet())
        {
            rscStateBuilder.addVlmStates(buildVolumeState(volumeEntry.getKey(), volumeEntry.getValue()));
        }

        return rscStateBuilder.build();
    }
}
