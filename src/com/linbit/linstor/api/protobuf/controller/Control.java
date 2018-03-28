package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.ApplicationLifecycleManager;
import com.linbit.linstor.proto.MsgControlCtrlOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_CONTROL_CTRL,
    description = "Send commands to the controller"
)
public class Control implements ApiCall
{
    private final AccessContext accCtx;
    private final ApplicationLifecycleManager applicationLifecycleManager;
    private final ApiCallAnswerer apiCallAnswerer;

    @Inject
    public Control(
        @PeerContext AccessContext accCtxRef,
        ApplicationLifecycleManager applicationLifecycleManagerRef,
        ApiCallAnswerer apiCallAnswererRef
    )
    {
        accCtx = accCtxRef;
        applicationLifecycleManager = applicationLifecycleManagerRef;
        apiCallAnswerer = apiCallAnswererRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
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
                    // Override MAC if the privilege is available
                    try
                    {
                        privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_MAC_OVRD);
                    }
                    catch (AccessDeniedException ignored)
                    {
                    }
                    // Override RBAC if the privilege is available
                    try
                    {
                        privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_OBJ_USE);
                    }
                    catch (AccessDeniedException ignored)
                    {
                    }
                    applicationLifecycleManager.requireShutdownAccess(privCtx);
                    ApiCallRcImpl.ApiCallRcEntry rcEntry = new ApiCallRcImpl.ApiCallRcEntry();
                    rcEntry.setMessageFormat("Controller will shutdown now.");
                    apiCallRc.addEntry(rcEntry);

                    // FIXME: The success message may not arrive at the client,
                    //        because sending it races with controller shutdown
                    apiCallAnswerer.answerApiCallRc(apiCallRc);
                    applicationLifecycleManager.shutdown(accCtx);
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

        apiCallAnswerer.answerApiCallRc(apiCallRc);
    }
}
