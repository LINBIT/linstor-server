package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.RscPojo;
import com.linbit.linstor.api.pojo.StorPoolPojo;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntFullSyncOuterClass.MsgIntFullSync;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntRscDataOuterClass.MsgIntRscData;
import com.linbit.linstor.proto.javainternal.MsgIntStorPoolDataOuterClass.MsgIntStorPoolData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class FullSync extends BaseProtoApiCall
{
    private Satellite satellite;

    public FullSync(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_FULL_SYNC_DATA;
    }

    @Override
    public String getDescription()
    {
        return "Transfers initial data for all objects to a satellite";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntFullSync fullSync = MsgIntFullSync.parseDelimitedFrom(msgDataIn);

        Set<NodePojo> nodes = new TreeSet<>(asNodes(fullSync.getNodesList()));
        Set<StorPoolPojo> storPools = new TreeSet<>(asStorPool(fullSync.getStorPoolsList()));
        Set<RscPojo> resources = new TreeSet<>(asResources(fullSync.getRscsList()));

        satellite.getApiCallHandler().applyFullSync(nodes, storPools, resources);
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
        for (MsgIntStorPoolData storPoolData : storPoolsList)
        {
            storPools.add(ApplyStorPool.asStorPoolPojo(storPoolData));
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
