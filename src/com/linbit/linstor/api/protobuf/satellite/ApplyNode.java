package com.linbit.linstor.api.protobuf.satellite;

import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.pojo.NetInterfacePojo;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.api.protobuf.ProtoMapUtils;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.StltApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NetIf;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NodeConn;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_NODE,
    description = "Applies node update data"
)
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
        MsgIntNodeData nodeData = MsgIntNodeData.parseDelimitedFrom(msgDataIn);
        NodePojo nodePojo = asNodePojo(nodeData);
        apiCallHandler.applyNodeChanges(nodePojo);
    }

    static NodePojo asNodePojo(MsgIntNodeData nodeData)
    {
        return new NodePojo(
            UUID.fromString(nodeData.getNodeUuid()),
            nodeData.getNodeName(),
            nodeData.getNodeType(),
            nodeData.getNodeFlags(),
            extractNetIfs(nodeData.getNodeNetIfsList()),
            extractNodeConns(nodeData.getNodeConnsList()),
            ProtoMapUtils.asMap(nodeData.getNodePropsList()),
            Peer.ConnectionStatus.ONLINE, // we just assume that we are connected to the other satellite / controller
            UUID.fromString(nodeData.getNodeDisklessStorPoolUuid()),
            nodeData.getFullSyncId(),
            nodeData.getUpdateId()
        );
    }

    static List<NetInterface.NetInterfaceApi> extractNetIfs(List<NetIf> nodeNetIfsList)
    {
        List<NetInterface.NetInterfaceApi> netIfs = new ArrayList<>();
        for (NetIf netIf : nodeNetIfsList)
        {
            netIfs.add(
                new NetInterfacePojo(
                    UUID.fromString(netIf.getNetIfUuid()),
                    netIf.getNetIfName(),
                    netIf.getNetIfAddr(),
                    netIf.getStltConnPort(),
                    netIf.getStltConnEncrType()
                )
            );
        }
        return netIfs;
    }

    static ArrayList<NodeConnPojo> extractNodeConns(List<NodeConn> nodeConnPropsList)
    {
        ArrayList<NodeConnPojo> nodeConns = new ArrayList<>();
        for (NodeConn nodeConn : nodeConnPropsList)
        {
            nodeConns.add(
                new NodeConnPojo(
                    UUID.fromString(nodeConn.getNodeConnUuid()),
                    UUID.fromString(nodeConn.getOtherNodeUuid()),
                    nodeConn.getOtherNodeName(),
                    nodeConn.getOtherNodeType(),
                    nodeConn.getOtherNodeFlags(),
                    ProtoMapUtils.asMap(nodeConn.getNodeConnPropsList()),
                    UUID.fromString(nodeConn.getOtherNodeDisklessStorPoolUuid())
                )
            );
        }
        return nodeConns;
    }

}
