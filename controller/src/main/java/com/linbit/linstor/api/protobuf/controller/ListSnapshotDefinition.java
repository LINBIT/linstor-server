package com.linbit.linstor.api.protobuf.controller;

import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_LST_SNAPSHOT_DFN,
    description = "Queries the list of snapshot definitions",
    transactional = false
)
@Singleton
public class ListSnapshotDefinition implements ApiCall
{
    private final CtrlApiCallHandler apiCallHandler;
    private final Provider<Peer> clientProvider;

    @Inject
    public ListSnapshotDefinition(CtrlApiCallHandler apiCallHandlerRef, Provider<Peer> clientProviderRef)
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
                .listSnapshotDefinition()
        );
    }
}
