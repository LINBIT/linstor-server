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
import com.linbit.linstor.security.Privilege;

@ProtobufApiCall
public class Control extends BaseProtoApiCall
{
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

        try
        {
            AccessContext privCtx = accCtx.clone();
            switch (msgControlCtrl.getCommand())
            {
                case ApiConsts.API_CMD_SHUTDOWN:
                {
                    try
                    {
                        privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_MAC_OVRD, Privilege.PRIV_OBJ_USE);
                    }
                    catch (AccessDeniedException ignored)
                    {
                    }
                    controller.requireShutdownAccess(privCtx);
                    ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
                    rcEntry.setMessageFormat("Controller will shutdown now.");
                    apiCallRc.addEntry(rcEntry);

                    // FIXME: The success message may not arrive at the client,
                    //        because sending it races with controller shutdown
                    this.answerApiCallRc(accCtx, client, msgId, apiCallRc);
                    controller.shutdown(accCtx);
                    break;
                }
                default:
                {
                    ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
                    rcEntry.setReturnCode(ApiConsts.MASK_ERROR | ApiConsts.UNKNOWN_API_CALL);
                    rcEntry.setMessageFormat(
                        String.format(
                            "API Controller command '%s' is unknown.",
                            msgControlCtrl.getCommand()
                        )
                    );
                    apiCallRc.addEntry(rcEntry);
                    break;
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.MASK_ERROR | ApiConsts.FAIL_ACC_DENIED_COMMAND);
            rcEntry.setMessageFormat(
                String.format(
                    "Role '%s' is not authorized for command '%s'.",
                    accCtx.subjectRole.name.displayValue,
                    msgControlCtrl.getCommand()
                )
            );
            apiCallRc.addEntry(rcEntry);
        }

        this.answerApiCallRc(accCtx, client, msgId, apiCallRc);
    }
}
