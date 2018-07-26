package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.FilterOuterClass.Filter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_STOR_POOL,
    description = "Queries the list of storage pools"
)
public class ListStorPool implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;

    @Inject
    public ListStorPool(CtrlApiCallHandler apiCallHandlerRef, Peer clientRef)
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
        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        if (filter != null)
        {
            nodeNames = filter.getNodeNamesList();
            storPoolNames = filter.getStorPoolNamesList();
        }

        client.sendMessage(
            apiCallHandler
                .listStorPool(nodeNames, storPoolNames)
        );
    }
}
