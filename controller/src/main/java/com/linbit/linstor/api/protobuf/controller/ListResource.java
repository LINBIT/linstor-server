package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiModule;
import com.linbit.linstor.api.interfaces.serializer.CtrlClientSerializer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ResourceList;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.common.FilterOuterClass.Filter;

import static com.linbit.linstor.api.ApiConsts.API_LST_RSC;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_RSC,
    description = "Queries the list of resources",
    transactional = false
)
@Singleton
public class ListResource implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Provider<Peer> clientProvider;
    private final Provider<Long> apiCallId;
    private final CtrlClientSerializer ctrlClientSerializer;

    @Inject
    public ListResource(
        CtrlApiCallHandler apiCallHandlerRef,
        Provider<Peer> clientProviderRef,
        @Named(ApiModule.API_CALL_ID) Provider<Long> apiCallIdRef,
        CtrlClientSerializer ctrlClientSerializerRef)
    {
        apiCallHandler = apiCallHandlerRef;
        clientProvider = clientProviderRef;
        apiCallId = apiCallIdRef;
        ctrlClientSerializer = ctrlClientSerializerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        List<String> nodeNames = new ArrayList<>();
        List<String> resourceNames = new ArrayList<>();

        Filter filter = Filter.parseDelimitedFrom(msgDataIn);
        if (filter != null)
        {
            nodeNames = filter.getNodeNamesList();
            resourceNames = filter.getResourceNamesList();
        }

        ResourceList rscList = apiCallHandler.listResource(nodeNames, resourceNames);

        byte[] response = ctrlClientSerializer
            .answerBuilder(API_LST_RSC, apiCallId.get())
            .resourceList(rscList.getResources(), rscList.getSatelliteStates())
            .build();

        clientProvider.get().sendMessage(response);
    }
}
