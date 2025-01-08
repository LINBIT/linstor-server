package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNetIf;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNode;
import com.linbit.linstor.proto.javainternal.c2s.IntNodeOuterClass.IntNodeConn;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyNodeOuterClass.MsgIntApplyNode;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_NODE,
    description = "Applies node update data"
)
@Singleton
public class ApplyNode implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyNode(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyNode applyNodeMsg = MsgIntApplyNode.parseDelimitedFrom(msgDataIn);
        NodePojo nodePojo = asNodePojo(
            applyNodeMsg.getNode(),
            applyNodeMsg.getFullSyncId(),
            applyNodeMsg.getUpdateId()
        );
        apiCallHandler.applyNodeChanges(nodePojo);
    }

    static NodePojo asNodePojo(IntNode nodeMsg, long fullSyncId, long updateId)
    {
        return new NodePojo(
            UUID.fromString(nodeMsg.getUuid()),
            nodeMsg.getName(),
            nodeMsg.getType(),
            nodeMsg.getFlags(),
            extractNetIfs(nodeMsg.getNetIfsList()),
            null,
            extractNodeConns(
                nodeMsg.getName(),
                nodeMsg.getNodeConnsList(),
                fullSyncId,
                updateId
            ),
            nodeMsg.getPropsMap(),
            // we just assume that we are connected to the other satellite / controller
            ApiConsts.ConnectionStatus.ONLINE,
            fullSyncId,
            updateId,
            null,
            null,
            null,
            null,
            null,
            0
        );
    }

    static List<NetInterfaceApi> extractNetIfs(List<IntNetIf> nodeNetIfsList)
    {
        List<NetInterfaceApi> netIfs = new ArrayList<>();
        for (IntNetIf netIf : nodeNetIfsList)
        {
            netIfs.add(
                new NetInterfacePojo(
                    UUID.fromString(netIf.getUuid()),
                    netIf.getName(),
                    netIf.getAddr(),
                    netIf.getStltConnPort(),
                    netIf.getStltConnEncrType()
                )
            );
        }
        return netIfs;
    }

    static ArrayList<NodeConnPojo> extractNodeConns(
        String localNodeNameRef,
        List<IntNodeConn> nodeConnPropsList,
        long fullSyncId,
        long updateId
    )
    {
        ArrayList<NodeConnPojo> nodeConns = new ArrayList<>();
        for (IntNodeConn nodeConn : nodeConnPropsList)
        {
            nodeConns.add(
                new NodeConnPojo(
                    UUID.fromString(nodeConn.getUuid()),
                    localNodeNameRef,
                    asNodePojo(nodeConn.getOtherNode(), fullSyncId, updateId),
                    nodeConn.getPropsMap()
                )
            );
        }
        return nodeConns;
    }

}
