package com.linbit.linstor.api.protobuf.controller.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.NetInterface;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnection;
import com.linbit.linstor.api.interfaces.serializer.CtrlNodeSerializer;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.proto.MsgHeaderOuterClass.MsgHeader;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.MsgIntNodeData;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NetIf;
import com.linbit.linstor.proto.javainternal.MsgIntNodeDataOuterClass.NodeConn;
import com.linbit.linstor.proto.javainternal.MsgIntObjectIdOuterClass.MsgIntObjectId;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.UuidUtils;

public class NodeDataSerializerProto implements CtrlNodeSerializer
{
    private final AccessContext serializerCtx;
    private final ErrorReporter errorReporter;

    public NodeDataSerializerProto(AccessContext serializerCtxRef, ErrorReporter errorReporterRef)
    {
        serializerCtx = serializerCtxRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public byte[] getChangedMessage(Node satelliteNode)
    {
        byte[] toSend = null;
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_CHANGED_NODE)
                .setMsgId(0)
                .build()
                .writeDelimitedTo(baos);

            MsgIntObjectId.newBuilder()
                .setName(satelliteNode.getName().displayValue)
                .setUuid(asByteString(satelliteNode.getUuid()))
                .build()
                .writeDelimitedTo(baos);

            toSend = baos.toByteArray();
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            toSend = new byte[0];
        }

        return toSend;
    }

    @Override
    public byte[] getDataMessage(int msgId, Node satelliteNode, Collection<Node> otherNodes)
    {
        byte[] toSend = null;
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            MsgHeader.newBuilder()
                .setApiCall(InternalApiConsts.API_APPLY_NODE)
                .setMsgId(0)
                .build()
                .writeDelimitedTo(baos);

            buildProtoMsg(satelliteNode, otherNodes)
                .writeDelimitedTo(baos);

            toSend = baos.toByteArray();
        }
        catch (IOException ioExc)
        {
            errorReporter.reportError(ioExc);
            toSend = new byte[0];
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    this.getClass().getSimpleName() + "'s serializerCtx had not enough privileges to " +
                    "serialize the node '" + satelliteNode.getName().displayValue + "'",
                    accDeniedExc
                )
            );
            toSend = new byte[0];
        }
        return toSend;
    }

    MsgIntNodeData buildProtoMsg(Node node, Collection<Node> relatedNodes)
        throws AccessDeniedException
    {
        return MsgIntNodeData.newBuilder()
            .setNodeUuid(asByteString(node.getUuid()))
            .setNodeName(node.getName().displayValue)
            .setNodeFlags(node.getFlags().getFlagsBits(serializerCtx))
            .setNodeType(node.getNodeType(serializerCtx).name())
            .addAllNodeNetIfs(getNetIfs(node))
            .addAllNodeConns(getNodeConns(node, relatedNodes))
            .addAllNodeProps(BaseProtoApiCall.fromMap(node.getProps(serializerCtx).map()))
            .build();
    }

    private Iterable<? extends NetIf> getNetIfs(Node node) throws AccessDeniedException
    {
        ArrayList<NetIf> netIfs = new ArrayList<>();
        Iterator<NetInterface> iterateNetInterfaces = node.iterateNetInterfaces(serializerCtx);
        while (iterateNetInterfaces.hasNext())
        {
            NetInterface netIf = iterateNetInterfaces.next();
            netIfs.add(
                NetIf.newBuilder()
                    .setNetIfUuid(asByteString(netIf.getUuid()))
                    .setNetIfName(netIf.getName().displayValue)
                    .setNetIfAddr(netIf.getAddress(serializerCtx).getAddress())
                    .setNetIfType(netIf.getNetInterfaceType(serializerCtx).name())
                    .setNetIfPort(netIf.getNetInterfacePort(serializerCtx))
                    .build()
            );
        }
        return netIfs;
    }

    private ArrayList<NodeConn> getNodeConns(Node node, Collection<Node> otherNodes)
        throws AccessDeniedException
    {
        ArrayList<NodeConn> nodeConns = new ArrayList<>();
        for (Node otherNode : otherNodes)
        {
           NodeConnection nodeConnection = node.getNodeConnection(serializerCtx, otherNode);
           String otherName;

           if (nodeConnection != null)
           {
               if (nodeConnection.getSourceNode(serializerCtx) == node)
               {
                   otherName = otherNode.getName().displayValue;
               }
               else
               {
                   otherName = node.getName().displayValue;
               }

               nodeConns.add(
                   NodeConn.newBuilder()
                       .setOtherNodeUuid(ByteString.copyFrom(UuidUtils.asByteArray(otherNode.getUuid())))
                       .setOtherNodeName(otherName)
                       .setOtherNodeType(otherNode.getNodeType(serializerCtx).name())
                       .setOtherNodeFlags(otherNode.getFlags().getFlagsBits(serializerCtx))
                       .setNodeConnUuid(ByteString.copyFrom(UuidUtils.asByteArray(nodeConnection.getUuid())))
                       .addAllNodeConnProps(
                           BaseProtoApiCall.fromMap(
                               nodeConnection.getProps(serializerCtx).map()
                           )
                       )
                       .build()
               );
           }
        }
        return nodeConns;
    }
    private ByteString asByteString(UUID uuid)
    {
        return ByteString.copyFrom(UuidUtils.asByteArray(uuid));
    }
}
