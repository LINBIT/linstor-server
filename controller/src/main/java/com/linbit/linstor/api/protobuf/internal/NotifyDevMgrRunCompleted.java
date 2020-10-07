package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCallReactive;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;
import com.linbit.linstor.netcom.Peer;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

import reactor.core.publisher.Flux;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_DEV_MGR_RUN_COMPLETED,
    description = "Called by the satellite to notify the controller that the last dev mgr run was completed"
)
@Singleton
public class NotifyDevMgrRunCompleted implements ApiCallReactive
{
    private final NodeInternalCallHandler nodeInternalCallHandler;
    private final Provider<Peer> peerProvider;

    @Inject
    public NotifyDevMgrRunCompleted(
        NodeInternalCallHandler nodeInternalCallHandlerRef,
        Provider<Peer> peerProviderRef
    )
    {
        nodeInternalCallHandler = nodeInternalCallHandlerRef;
        peerProvider = peerProviderRef;
    }

    @Override
    public Flux<byte[]> executeReactive(InputStream msgDataInRef) throws IOException
    {
        return nodeInternalCallHandler.handleDevMgrRunCompleted(peerProvider.get().getNode())
            .thenMany(Flux.empty());
    }
}
