package com.linbit.linstor.api.protobuf.satellite;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.satellite.StltApiCallHandler;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.NetInterfaceApi;
import com.linbit.linstor.netcom.Peer;
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
            extractNodeConns(nodeMsg.getNodeConnsList()),
            nodeMsg.getPropsMap(),
            Peer.ConnectionStatus.ONLINE, // we just assume that we are connected to the other satellite / controller
            fullSyncId,
            updateId
        );
    }

    static List<NetInterface.NetInterfaceApi> extractNetIfs(List<IntNetIf> nodeNetIfsList)
    {
        List<NetInterface.NetInterfaceApi> netIfs = new ArrayList<>();
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

    static ArrayList<NodeConnPojo> extractNodeConns(List<IntNodeConn> nodeConnPropsList)
    {
        ArrayList<NodeConnPojo> nodeConns = new ArrayList<>();
        for (IntNodeConn nodeConn : nodeConnPropsList)
        {
            nodeConns.add(
                new NodeConnPojo(
                    UUID.fromString(nodeConn.getNodeConnUuid()),
                    UUID.fromString(nodeConn.getOtherNodeUuid()),
                    nodeConn.getOtherNodeName(),
                    nodeConn.getOtherNodeType(),
                    nodeConn.getOtherNodeFlags(),
                    nodeConn.getNodeConnPropsMap()
                )
            );
        }
        return nodeConns;
    }

}
