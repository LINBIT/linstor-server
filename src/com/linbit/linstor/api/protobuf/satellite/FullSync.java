package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.core.StltApiCallHandlerUtils;
import com.linbit.linstor.core.UpdateMonitor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.StorPoolFreeSpaceOuterClass.StorPoolFreeSpace;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncSuccessOuterClass.MsgIntFullSyncSuccess;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.storage.StorageException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_DATA,
    description = "Transfers initial data for all objects to a satellite"
)
public class FullSync implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final StltApiCallHandlerUtils apiCallHandlerUtils;
    private final ApiCallAnswerer apiCallAnswerer;
    private final ControllerPeerConnector controllerPeerConnector;
    private final Peer controllerPeer;
    private final ErrorReporter errorReporter;
    private final UpdateMonitor updateMonitor;

    @Inject
    public FullSync(
        StltApiCallHandler apiCallHandlerRef,
        StltApiCallHandlerUtils apiCallHandlerUtilsRef,
        ApiCallAnswerer apiCallAnswererRef,
        ControllerPeerConnector controllerPeerConnectorRef,
        Peer controllerPeerRef,
        ErrorReporter errorReporterRef,
        UpdateMonitor updateMonitorRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        apiCallHandlerUtils = apiCallHandlerUtilsRef;
        apiCallAnswerer = apiCallAnswererRef;
        controllerPeerConnector = controllerPeerConnectorRef;
        controllerPeer = controllerPeerRef;
        errorReporter = errorReporterRef;
        updateMonitor = updateMonitorRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntFullSync fullSync = MsgIntFullSync.parseDelimitedFrom(msgDataIn);

        Set<NodePojo> nodes = new TreeSet<>(asNodes(fullSync.getNodesList()));
        Set<StorPoolPojo> storPools = new TreeSet<>(asStorPool(fullSync.getStorPoolsList()));
        Set<RscPojo> resources = new TreeSet<>(asResources(fullSync.getRscsList()));

        apiCallHandler.applyFullSync(
            nodes,
            storPools,
            resources,
            fullSync.getFullSyncTimestamp()
        );

        Map<StorPool, Long> freeSpaceMap;
        try
        {
            freeSpaceMap = apiCallHandlerUtils.getFreeSpace();
            MsgIntFullSyncSuccess.Builder builder = MsgIntFullSyncSuccess.newBuilder();
            for (Entry<StorPool, Long> entry : freeSpaceMap.entrySet())
            {
                StorPool storPool = entry.getKey();
                builder.addFreeSpace(
                    StorPoolFreeSpace.newBuilder()
                        .setStorPoolUuid(storPool.getUuid().toString())
                        .setStorPoolName(storPool.getName().displayValue)
                        .setFreeSpace(entry.getValue())
                        .build()
                );
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            builder.build().writeDelimitedTo(baos);
            controllerPeer.sendMessage(
                apiCallAnswerer.prepareMessage(
                    baos.toByteArray(),
                    InternalApiConsts.API_FULL_SYNC_SUCCESS
                )
            );
        }
        catch (StorageException storageExc)
        {
            // TODO: report about this error to the controller
            errorReporter.reportError(storageExc);
        }
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

}
