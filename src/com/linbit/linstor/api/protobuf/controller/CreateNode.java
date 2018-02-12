package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.SatelliteConnection.SatelliteConnectionApi;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.SatelliteConnectionPojo;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgCrtNodeOuterClass.MsgCrtNode;
import com.linbit.linstor.proto.NetInterfaceOuterClass;
import com.linbit.linstor.proto.NodeOuterClass;
import com.linbit.linstor.proto.apidata.NetInterfaceApiData;
import com.linbit.linstor.security.AccessContext;

@ProtobufApiCall
public class CreateNode extends BaseProtoApiCall
{
    private final Controller controller;

    public CreateNode(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CRT_NODE;
    }

    @Override
    public String getDescription()
    {
        return "Creates a node";
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgCrtNode msgCreateNode = MsgCrtNode.parseDelimitedFrom(msgDataIn);
        NodeOuterClass.Node protoNode = msgCreateNode.getNode();
        ApiCallRc apiCallRc = controller.getApiCallHandler().createNode(
            accCtx,
            client,
            // nodeUuid is ignored here
            protoNode.getName(),
            protoNode.getType(),
            extractNetIfs(protoNode.getNetInterfacesList()),
            extractSatelliteConnections(protoNode.getNetInterfacesList()),
            asMap(protoNode.getPropsList())
        );
        answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }

    private List<NetInterfaceApi> extractNetIfs(List<NetInterfaceOuterClass.NetInterface> protoNetIfs)
    {
        List<NetInterfaceApi> netIfs = new ArrayList<>();
        for (NetInterfaceOuterClass.NetInterface protoNetIf : protoNetIfs)
        {
            netIfs.add(new NetInterfaceApiData(protoNetIf));
        }
        return netIfs;
    }

    private List<SatelliteConnectionApi> extractSatelliteConnections(
        List<NetInterfaceOuterClass.NetInterface> protoNetIfs
    )
    {
        List<SatelliteConnectionApi> stltConnList = new ArrayList<>();
        for (NetInterfaceOuterClass.NetInterface netIf : protoNetIfs)
        {
            if (netIf.hasStltEncryptionType() && netIf.hasStltPort())
            {
                stltConnList.add(
                    new SatelliteConnectionPojo(
                        netIf.getName(),
                        netIf.getStltPort(),
                        netIf.getStltEncryptionType()
                    )
                );
            }
            // TODO: if only one is set, maybe print a warning or something
        }
        return stltConnList;
    }
}
