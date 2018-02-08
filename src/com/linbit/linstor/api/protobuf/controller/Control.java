package com.linbit.linstor.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.BaseProtoApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.Controller;
import com.linbit.linstor.netcom.Message;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgControlCtrlOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

@ProtobufApiCall
public class Control extends BaseProtoApiCall {
    private final Controller controller;

    public Control(Controller controllerRef)
    {
        super(controllerRef.getErrorReporter());
        controller = controllerRef;
    }

    @Override
    public String getName()
    {
        return ApiConsts.API_CONTROL_CTRL;
    }

    @Override
    public String getDescription()
    {
        return "Send commands to the controller";
    }

    @Override
    protected void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
        throws IOException
    {
        MsgControlCtrlOuterClass.MsgControlCtrl msgControlCtrl =
            MsgControlCtrlOuterClass.MsgControlCtrl.parseDelimitedFrom(msgDataIn);

        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();

        try {
            switch (msgControlCtrl.getCommand()) {
                case ApiConsts.API_CMD_SHUTDOWN: {
                    controller.hasShutdownAccess(accCtx);
                    ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
                    rcEntry.setMessageFormat("Controller will shutdown now.");
                    apiCallRc.addEntry(rcEntry);

                    this.answerApiCallRc(accCtx, client, msgId, apiCallRc); // send success message now
                    controller.shutdown(accCtx);
                    return;
                }
                default: {
                    ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
                    rcEntry.setReturnCode(ApiConsts.MASK_ERROR | ApiConsts.UNKNOWN_API_CALL);
                    rcEntry.setMessageFormat(
                        String.format("API Controller command '%s' is unknown.",
                            msgControlCtrl.getCommand())
                    );
                    apiCallRc.addEntry(rcEntry);
                }
            }
        }
        catch (AccessDeniedException accDenied) {
            ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.MASK_ERROR | ApiConsts.FAIL_ACC_DENIED_COMMAND);
            rcEntry.setMessageFormat(
                String.format("Access to command '%s' is denied with the current user context.",
                    msgControlCtrl.getCommand())
            );
            apiCallRc.addEntry(rcEntry);
        }

        this.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
