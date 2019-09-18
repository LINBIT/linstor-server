package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.SnapshotPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.api.protobuf.serializer.ProtoCtrlStltSerializerBuilder;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandlerUtils;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.c2s.IntControllerOuterClass.IntController;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNode;
import com.linbit.linstor.proto.javainternal.c2s.IntRscOuterClass.IntRsc;
import com.linbit.linstor.proto.javainternal.c2s.IntSnapshotOuterClass;
import com.linbit.linstor.proto.javainternal.c2s.IntStorPoolOuterClass.IntStorPool;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyFullSyncOuterClass.MsgIntApplyFullSync;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntFullSyncResponseOuterClass.MsgIntFullSyncResponse;
import com.linbit.utils.Base64;
import com.linbit.utils.Either;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

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
        MsgIntApplyFullSync applyFullSync = MsgIntApplyFullSync.parseDelimitedFrom(msgDataIn);
        long fullSyncId = applyFullSync.getFullSyncTimestamp();
        long updateId = 0;

        IntController msgIntControllerData = applyFullSync.getCtrl();
        Set<NodePojo> nodes = new TreeSet<>(asNodes(applyFullSync.getNodesList(), fullSyncId, updateId));
        Set<StorPoolPojo> storPools = new TreeSet<>(asStorPool(applyFullSync.getStorPoolsList(), fullSyncId, updateId));
        Set<RscPojo> resources = new TreeSet<>(asResources(applyFullSync.getRscsList(), fullSyncId, updateId));
        Set<SnapshotPojo> snapshots = new TreeSet<>(
            asSnapshots(
                applyFullSync.getSnapshotsList(),
                fullSyncId,
                updateId
            )
        );

        boolean success = apiCallHandler.applyFullSync(
            msgIntControllerData.getPropsMap(),
            nodes,
            storPools,
            resources,
            snapshots,
            applyFullSync.getFullSyncTimestamp(),
            Base64.decode(applyFullSync.getMasterKey())
        );

        MsgIntFullSyncResponse.Builder builder = MsgIntFullSyncResponse.newBuilder();
        builder.setSuccess(success);
        if (success)
        {
            Map<StorPool, Either<SpaceInfo, ApiRcException>> spaceInfoQueryMap =
                apiCallHandlerUtils.getAllSpaceInfo(false);

            for (Entry<StorPool, Either<SpaceInfo, ApiRcException>> entry : spaceInfoQueryMap.entrySet())
            {
                builder.addFreeSpace(ProtoCtrlStltSerializerBuilder.buildStorPoolFreeSpace(entry).build());
            }
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        builder.build().writeDelimitedTo(baos);
        controllerPeerProvider.get().sendMessage(
            apiCallAnswerer.answerBytes(
                baos.toByteArray(),
                InternalApiConsts.API_FULL_SYNC_RESPONSE
            )
        );
    }

    private ArrayList<NodePojo> asNodes(
        List<IntNode> nodesList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<NodePojo> nodes = new ArrayList<>(nodesList.size());

        for (IntNode node : nodesList)
        {
            nodes.add(ApplyNode.asNodePojo(node, fullSyncId, updateId));
        }
        return nodes;
    }

    private ArrayList<StorPoolPojo> asStorPool(
        List<IntStorPool> storPoolsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<StorPoolPojo> storPools = new ArrayList<>(storPoolsList.size());
        String nodeName = controllerPeerConnector.getLocalNode().getName().displayValue;
        for (IntStorPool storPool : storPoolsList)
        {
            storPools.add(ApplyStorPool.asStorPoolPojo(storPool, nodeName, fullSyncId, updateId));
        }
        return storPools;
    }

    private ArrayList<RscPojo> asResources(
        List<IntRsc> rscsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<RscPojo> rscs = new ArrayList<>(rscsList.size());
        for (IntRsc rscData : rscsList)
        {
            rscs.add(ApplyRsc.asRscPojo(rscData, fullSyncId, updateId));
        }
        return rscs;
    }

    private ArrayList<SnapshotPojo> asSnapshots(
        List<IntSnapshotOuterClass.IntSnapshot> snapshotsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<SnapshotPojo> snapshots = new ArrayList<>(snapshotsList.size());
        for (IntSnapshotOuterClass.IntSnapshot snapshot : snapshotsList)
        {
            snapshots.add(ApplySnapshot.asSnapshotPojo(snapshot, fullSyncId, updateId));
        }
        return snapshots;
    }

}
