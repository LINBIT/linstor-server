package com.linbit.linstor.api.protobuf.internal;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.apicallhandler.controller.internal.NodeInternalCallHandler;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_NOTIFY_DEV_MGR_RUN_COMPLETED,
    description = "Called by the satellite to notify the controller that the last dev mgr run was completed",
    transactional = true
)
@Singleton
public class NotifyDevMgrRunCompleted implements ApiCall
{
    private final NodeInternalCallHandler nodeInternalCallHandler;

    @Inject
    public NotifyDevMgrRunCompleted(
        NodeInternalCallHandler nodeInternalCallHandlerRef
    )
    {
        nodeInternalCallHandler = nodeInternalCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataInRef) throws IOException
    {
        nodeInternalCallHandler.handleDevMgrRunCompleted();

    }
}
