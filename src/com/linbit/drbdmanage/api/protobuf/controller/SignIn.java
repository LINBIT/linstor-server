package com.linbit.drbdmanage.api.protobuf.controller;

import java.io.IOException;
import java.io.InputStream;

import com.linbit.InvalidNameException;
import com.linbit.drbdmanage.api.ApiCallRcImpl;
import com.linbit.drbdmanage.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.api.protobuf.BaseProtoApiCall;
import com.linbit.drbdmanage.api.protobuf.ProtobufApiCall;
import com.linbit.drbdmanage.core.Controller;
import com.linbit.drbdmanage.netcom.Message;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.proto.MsgSignInOuterClass.MsgSignIn;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.IdentityName;
import com.linbit.drbdmanage.security.SignInException;

@ProtobufApiCall
public class SignIn extends BaseProtoApiCall
{
    private final Controller ctrl;
    public SignIn(Controller ctrlRef)
    {
        super(ctrlRef.getErrorReporter());
        ctrl = ctrlRef;
    }

    @Override
    public String getName()
    {
        return SignIn.class.getSimpleName();
    }

    @Override
    public String getDescription()
    {
        return "Performs a sign-in with the specified credentials";
    }

    @Override
    public void executeImpl(
        AccessContext accCtx,
        Message msg,
        int msgId,
        InputStream msgDataIn,
        Peer client
    )
    {
        ApiCallRcImpl reply = new ApiCallRcImpl();
        String idNameText = null;
        try
        {
            MsgSignIn signInData = MsgSignIn.parseDelimitedFrom(msgDataIn);
            idNameText = signInData.getIdName();
            byte[] password = signInData.getPasswordBytes().toByteArray();

            IdentityName idName = new IdentityName(idNameText);

            ctrl.peerSignIn(client, idName, password);

            AccessContext clientCtx = client.getAccessContext();

            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setMessageFormat("Sign-in successful");
            rcEntry.putVariable(ApiConsts.KEY_SEC_IDENTITY, clientCtx.subjectId.name.displayValue);
            rcEntry.putVariable(ApiConsts.KEY_SEC_ROLE, clientCtx.subjectRole.name.displayValue);
            rcEntry.putVariable(ApiConsts.KEY_SEC_DOMAIN, clientCtx.subjectDomain.name.displayValue);
            reply.addEntry(rcEntry);
        }
        catch (IOException ioExc)
        {
            String reportId = errorReporter.reportError(ioExc, accCtx, client, "Sign-in");
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.RC_SIGNIN_FAIL);
            rcEntry.setMessageFormat("Sgn-in failed");
            rcEntry.setCauseFormat("The sign-in request could not be processed due to an I/O error");
            rcEntry.setCorrectionFormat(
                "If this problem persists, refer to the problem report on the server system for more detailed " +
                "information about the problem and possible solutions"
            );
            if (reportId != null)
            {
                rcEntry.setDetailsFormat("A problem report was filed under report ID " + reportId);
            }
            if (idNameText != null)
            {
                rcEntry.putVariable(ApiConsts.KEY_SEC_IDENTITY, idNameText);
            }
            reply.addEntry(rcEntry);
        }
        catch (InvalidNameException nameExc)
        {
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.RC_SIGNIN_FAIL);
            rcEntry.setMessageFormat(nameExc.getMessage());
            if (idNameText != null)
            {
                rcEntry.putVariable(ApiConsts.KEY_SEC_IDENTITY, idNameText);
            }
            reply.addEntry(rcEntry);
        }
        catch (SignInException signInExc)
        {
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.RC_SIGNIN_FAIL);
            rcEntry.setMessageFormat(signInExc.getDescriptionText());
            rcEntry.setCauseFormat(signInExc.getCauseText());
            rcEntry.setCorrectionFormat(signInExc.getCorrectionText());
            rcEntry.setDetailsFormat(signInExc.getDetailsText());
            if (idNameText != null)
            {
                rcEntry.putVariable(ApiConsts.KEY_SEC_IDENTITY, idNameText);
            }
            reply.addEntry(rcEntry);
        }

        answerApiCallRc(accCtx, client, msgId, reply);
    }
}
