package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author rpeinthor
 */
@ProtobufApiCall(
    name = ApiConsts.API_LST_STOR_POOL_DFN,
    description = "Queries the list of storage pool definitions",
    transactional = false
)
@Singleton
public class ListStorPoolDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Provider<Peer> clientProvider;

    @Inject
    public ListStorPoolDefinition(CtrlApiCallHandler apiCallHandlerRef, Provider<Peer> clientProviderRef)
    {
        apiCallHandler = apiCallHandlerRef;
        clientProvider = clientProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        clientProvider.get().sendMessage(
            apiCallHandler
                .listStorPoolDefinition()
        );
    }
}
