package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlStorPoolListApiCallHandler;
import com.linbit.linstor.proto.FilterOuterClass.Filter;
import reactor.core.publisher.Flux;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_STOR_POOL,
    description = "Queries the list of storage pools"
)
@Singleton
public class ListStorPool implements ApiCallReactive
{
    private final CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandler;

    @Inject
    public ListStorPool(CtrlStorPoolListApiCallHandler ctrlStorPoolListApiCallHandlerRef)
    {
        ctrlStorPoolListApiCallHandler = ctrlStorPoolListApiCallHandlerRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataIn)
        throws IOException
    {
        List<String> nodeNames = Collections.emptyList();
        List<String> storPoolNames = Collections.emptyList();
        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        if (filter != null)
        {
            nodeNames = filter.getNodeNamesList();
            storPoolNames = filter.getStorPoolNamesList();
        }

        return ctrlStorPoolListApiCallHandler.listStorPools(nodeNames, storPoolNames);
    }
}
