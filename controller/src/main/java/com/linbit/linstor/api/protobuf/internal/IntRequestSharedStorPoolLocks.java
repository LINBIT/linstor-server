package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntRequestSharedStorPoolLocksOuterClass.MsgIntRequestSharedStorPoolLocks;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@ProtobufApiCall(
    name = InternalApiConsts.API_REQUEST_SHARED_SP_LOCKS,
    description = "Called by the satellite to request shared SP locks required for the next devMgr run",
    transactional = true
)
@Singleton
public class IntRequestSharedStorPoolLocks implements ApiCall
{
    private final NodeInternalCallHandler nodeInternalCallHandler;

    @Inject
    public IntRequestSharedStorPoolLocks(NodeInternalCallHandler apiCallHandlerRef)
    {
        nodeInternalCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntRequestSharedStorPoolLocks msg = MsgIntRequestSharedStorPoolLocks.parseDelimitedFrom(msgDataIn);
        List<String> sharedStorPoolLocksList = msg.getSharedStorPoolLocksList();

        nodeInternalCallHandler.handleSharedStorPoolLockRequest(sharedStorPoolLocksList);
    }
}
