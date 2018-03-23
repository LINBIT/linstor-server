package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.FilterOuterClass.Filter;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_VLM,
    description = "Queries the list of volumes"
)
public class ListVolumes implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public ListVolumes(CtrlApiCallHandler apiCallHandlerRef, Peer clientRef)
    {
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        client.sendMessage(
            apiCallHandler
                .listVolumes(filter.getNodeNamesList(), filter.getStorPoolNamesList(), filter.getResourceNamesList())
        );
    }
}
