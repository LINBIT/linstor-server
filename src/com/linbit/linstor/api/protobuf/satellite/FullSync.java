package com.linbit.linstor.api.protobuf.satellite;

import com.google.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@ProtobufApiCall(
    name = InternalApiConsts.API_FULL_SYNC_DATA,
    description = "Transfers initial data for all objects to a satellite"
)
public class FullSync implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;
    private final ControllerPeerConnector controllerPeerConnector;

    @Inject
    public FullSync(
        StltApiCallHandler apiCallHandlerRef,
        ControllerPeerConnector controllerPeerConnectorRef
    )
    {
        apiCallHandler = apiCallHandlerRef;
        controllerPeerConnector = controllerPeerConnectorRef;
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
