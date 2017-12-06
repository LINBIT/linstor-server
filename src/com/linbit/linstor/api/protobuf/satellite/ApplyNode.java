package com.linbit.linstor.api.protobuf.satellite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.pojo.NodePojo;
import com.linbit.linstor.api.pojo.NodePojo.NetIfPojo;
import com.linbit.linstor.api.pojo.NodePojo.NodeConnPojo;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Satellite;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NetIf;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NodeConn;
import com.linbit.linstor.security.AccessContext;
import com.linbit.utils.UuidUtils;

@ProtobufApiCall
public class ApplyNode extends BaseProtoApiCall
{
    private Satellite satellite;

    public ApplyNode(Satellite satellite)
    {
        super(satellite.getErrorReporter());
        this.satellite = satellite;
    }

    @Override
    public String getName()
    {
        return InternalApiConsts.API_APPLY_NODE;
    }

    @Override
    public String getDescription()
    {
        return "Applies the changes for the given node";
    }

    @Override
    protected void executeImpl(AccessContext accCtx, Message msg, int msgId, InputStream msgDataIn, Peer client)
        throws IOException
    {
        MsgIntNodeData nodeData = MsgIntNodeData.parseDelimitedFrom(msgDataIn);
        NodePojo nodePojo = asNodePojo(nodeData);
        satellite.getApiCallHandler().applyNodeChanges(nodePojo);
    }

    static NodePojo asNodePojo(MsgIntNodeData nodeData)
    {
        return new NodePojo(
            UuidUtils.asUuid(nodeData.getNodeUuid().toByteArray()),
            nodeData.getNodeName(),
            nodeData.getNodeType(),
            nodeData.getNodeFlags(),
            extractNetIfs(nodeData.getNodeNetIfsList()),
            extractNodeConns(nodeData.getNodeConnsList()),
            BaseProtoApiCall.asMap(nodeData.getNodePropsList())
        );
    }

    static List<NetIfPojo> extractNetIfs(List<NetIf> nodeNetIfsList)
    {
        List<NetIfPojo> netIfs = new ArrayList<>();
        for (NetIf netIf : nodeNetIfsList)
        {
            netIfs.add(
                new NetIfPojo(
                    UuidUtils.asUuid(netIf.getNetIfUuid().toByteArray()),
                    netIf.getNetIfName(),
                    netIf.getNetIfAddr(),
                    netIf.getNetIfType(),
                    netIf.getNetIfPort()
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
                    UuidUtils.asUuid(nodeConn.getNodeConnUuid().toByteArray()),
                    UuidUtils.asUuid(nodeConn.getOtherNodeUuid().toByteArray()),
                    nodeConn.getOtherNodeName(),
                    nodeConn.getOtherNodeType(),
                    nodeConn.getOtherNodeFlags(),
                    BaseProtoApiCall.asMap(nodeConn.getNodeConnPropsList())
                )
            );
        }
        return nodeConns;
    }

}
