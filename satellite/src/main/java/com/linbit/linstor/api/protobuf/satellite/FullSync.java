package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntControllerDataOuterClass.MsgIntControllerData;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncSuccessOuterClass.MsgIntFullSyncSuccess;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntSnapshotDataOuterClass;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.utils.Base64;
import com.linbit.utils.Either;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_DATA,
    description = "Transfers initial data for all objects to a satellite"
)
@Singleton
public class FullSync implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final ApiCallAnswerer apiCallAnswerer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Provider<Peer> controllerPeerProvider;
    private final ErrorReporter errorReporter;

    @Inject
    public FullSync(
        StltApiCallHandler apiCallHandlerRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        ApiCallAnswerer apiCallAnswererRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Provider<Peer> controllerPeerProviderRef,
        ErrorReporter errorReporterRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        apiCallAnswerer = apiCallAnswererRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        controllerPeerProvider = controllerPeerProviderRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntFullSync fullSync = MsgIntFullSync.parseDelimitedFrom(msgDataIn);

        MsgIntControllerData msgIntControllerData = fullSync.getCtrlData();
        Set<NodePojo> nodes = new TreeSet<>(asNodes(fullSync.getNodesList()));
        Set<StorPoolPojo> storPools = new TreeSet<>(asStorPool(fullSync.getStorPoolsList()));
        Set<RscPojo> resources = new TreeSet<>(asResources(fullSync.getRscsList()));
        Set<SnapshotPojo> snapshots = new TreeSet<>(asSnapshots(fullSync.getSnapshotsList()));

        apiCallHandler.applyFullSync(
            ProtoMapUtils.asMap(msgIntControllerData.getControllerPropsList()),
            nodes,
            storPools,
            resources,
            snapshots,
            fullSync.getFullSyncTimestamp(),
            Base64.decode(fullSync.getMasterKey())
        );

        Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
            apiCallHandlerUtils.getAllSpaceInfo(false);

        Map<StorPool, SpaceInfo> spaceInfoMap = new TreeMap<>();

        spaceInfoQueryMap.forEach((storPool, either) -> either.consume(
            spaceInfo -> spaceInfoMap.put(storPool, spaceInfo),
            apiRcException -> errorReporter.reportError(apiRcException.getCause())
        ));

        MsgIntFullSyncSuccess.Builder builder = MsgIntFullSyncSuccess.newBuilder();
        for (Entry<StorPool, SpaceInfo> entry : spaceInfoMap.entrySet())
        {
            StorPool storPool = entry.getKey();
            builder.addFreeSpace(
                StorPoolFreeSpace.newBuilder()
                    .setStorPoolUuid(storPool.getUuid().toString())
                    .setStorPoolName(storPool.getName().displayValue)
                    .setFreeCapacity(entry.getValue().freeCapacity)
                    .setTotalCapacity(entry.getValue().totalCapacity)
                    .build()
            );
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        builder.build().writeDelimitedTo(baos);
        controllerPeerProvider.get().sendMessage(
            apiCallAnswerer.prepareOnewayMessage(
                baos.toByteArray(),
                InternalApiConsts.API_FULL_SYNC_SUCCESS
            )
        );
    }

    private ArrayList<NodePojo> asNodes(List<MsgIntNodeData> nodesList)
    {
        ArrayList<NodePojo> nodes = new ArrayList<>(nodesList.size());

        for (MsgIntNodeData nodeData : nodesList)
        {
            nodes.add(ApplyNode.asNodePojo(nodeData));
        }
        return nodes;
    }

    private ArrayList<StorPoolPojo> asStorPool(List<MsgIntStorPoolData> storPoolsList)
    {
        ArrayList<StorPoolPojo> storPools = new ArrayList<>(storPoolsList.size());
        String nodeName = controllerPeerConnector.getLocalNode().getName().displayValue;
        for (MsgIntStorPoolData storPoolData : storPoolsList)
        {
            storPools.add(ApplyStorPool.asStorPoolPojo(storPoolData, nodeName));
        }
        return storPools;
    }

    private ArrayList<RscPojo> asResources(List<MsgIntRscData> rscsList)
    {
        ArrayList<RscPojo> rscs = new ArrayList<>(rscsList.size());
        for (MsgIntRscData rscData : rscsList)
        {
            rscs.add(ApplyRsc.asRscPojo(rscData));
        }
        return rscs;
    }

    private ArrayList<SnapshotPojo> asSnapshots(List<MsgIntSnapshotDataOuterClass.MsgIntSnapshotData> snapshotsList)
    {
        ArrayList<SnapshotPojo> snapshots = new ArrayList<>(snapshotsList.size());
        for (MsgIntSnapshotDataOuterClass.MsgIntSnapshotData snapshotData : snapshotsList)
        {
            snapshots.add(ApplySnapshot.asSnapshotPojo(snapshotData));
        }
        return snapshots;
    }

}
