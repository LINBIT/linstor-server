package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
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
        List<String> nodeNames = new ArrayList<>();
        List<String> storPoolNames = new ArrayList<>();
        List<String> resourceNames = new ArrayList<>();

        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        if (filter != null)
        {
            nodeNames = filter.getNodeNamesList();
            storPoolNames = filter.getStorPoolNamesList();
            resourceNames = filter.getResourceNamesList();
        }
        client.sendMessage(
            apiCallHandler
                .listVolumes(nodeNames, storPoolNames, resourceNames)
        );
    }
}
