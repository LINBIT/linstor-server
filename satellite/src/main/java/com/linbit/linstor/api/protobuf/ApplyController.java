package com.linbit.linstor.api.protobuf;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.core.apicallhandler.StltApiCallHandler;
import com.linbit.linstor.proto.javainternal.c2s.MsgIntApplyControllerOuterClass.MsgIntApplyController;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = InternalApiConsts.API_APPLY_CONTROLLER,
    description = "Applies controller update data"
)
@Singleton
public class ApplyController implements ApiCall
{
    private final StltApiCallHandler apiCallHandler;

    @Inject
    public ApplyController(StltApiCallHandler apiCallHandlerRef)
    {
        apiCallHandler = apiCallHandlerRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntApplyController controllerData = MsgIntApplyController.parseDelimitedFrom(msgDataIn);
        apiCallHandler.applyControllerChanges(
            controllerData.getCtrl().getPropsMap(),
            controllerData.getFullSyncId(),
            controllerData.getUpdateId()
        );
    }
}
