package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler;
import com.linbit.linstor.proto.FilterOuterClass.Filter;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_VLM,
    description = "Queries the list of volumes"
)
@Singleton
public class ListVolumes implements ApiCallReactive
{
    private final CtrlVlmListApiCallHandler ctrlVlmListApiCallHandler;

    @Inject
    public ListVolumes(CtrlVlmListApiCallHandler ctrlVlmListApiCallHandlerRef)
    {
        ctrlVlmListApiCallHandler = ctrlVlmListApiCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
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

        return ctrlVlmListApiCallHandler.listVlms(nodeNames, storPoolNames, resourceNames);
    }
}
