package com.linbit.linstor.api.protobuf.controller;

import javax.inject.Inject;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.protobuf.ApiCallAnswerer;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.MsgSignInOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Authentication;
import com.linbit.linstor.security.IdentityName;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SignInException;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = "SignIn",
    description = "Performs a sign-in with the specified credentials",
    requiresAuth = false
)
public class SignIn implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final Authentication idAuthentication;
    private final ApiCallAnswerer apiCallAnswerer;
    private final AccessContext sysCtx;
    private final AccessContext clientCtx;
    private final Peer client;

    @Inject
    public SignIn(
        ErrorReporter errorReporterRef,
        Authentication idAuthenticationRef,
        ApiCallAnswerer apiCallAnswererRef,
        @SystemContext AccessContext sysCtxRef,
        @PeerContext AccessContext clientCtxRef,
        Peer clientRef
    )
    {
        errorReporter = errorReporterRef;
        idAuthentication = idAuthenticationRef;
        apiCallAnswerer = apiCallAnswererRef;
        sysCtx = sysCtxRef;
        clientCtx = clientCtxRef;
        client = clientRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
    {
        ApiCallRcImpl reply = new ApiCallRcImpl();
        String idNameText = null;
        try
        {
            MsgSignInOuterClass.MsgSignIn signInData = MsgSignInOuterClass.MsgSignIn.parseDelimitedFrom(msgDataIn);
            idNameText = signInData.getIdName();
            byte[] password = signInData.getPasswordBytes().toByteArray();

            IdentityName idName = new IdentityName(idNameText);

            peerSignIn(idName, password);

            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setMessage("Sign-in successful");
            rcEntry.putObjRef(ApiConsts.KEY_SEC_IDENTITY, clientCtx.subjectId.name.displayValue);
            rcEntry.putObjRef(ApiConsts.KEY_SEC_ROLE, clientCtx.subjectRole.name.displayValue);
            rcEntry.putObjRef(ApiConsts.KEY_SEC_DOMAIN, clientCtx.subjectDomain.name.displayValue);
            reply.addEntry(rcEntry);
        }
        catch (IOException ioExc)
        {
            String reportId = errorReporter.reportError(ioExc, clientCtx, client, "Sign-in");
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.RC_SIGNIN_FAIL);
            rcEntry.setMessage("Sgn-in failed");
            rcEntry.setCause("The sign-in request could not be processed due to an I/O error");
            rcEntry.setCorrection(
                "If this problem persists, refer to the problem report on the server system for more detailed " +
                "information about the problem and possible solutions"
            );
            if (reportId != null)
            {
                rcEntry.setDetails("A problem report was filed under report ID " + reportId);
            }
            reply.addEntry(rcEntry);
        }
        catch (InvalidNameException nameExc)
        {
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.RC_SIGNIN_FAIL);
            rcEntry.setMessage(nameExc.getMessage());
            if (idNameText != null)
            {
                rcEntry.putObjRef(ApiConsts.KEY_SEC_IDENTITY, idNameText);
            }
            reply.addEntry(rcEntry);
        }
        catch (SignInException signInExc)
        {
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.RC_SIGNIN_FAIL);
            rcEntry.setMessage(signInExc.getDescriptionText());
            rcEntry.setCause(signInExc.getCauseText());
            rcEntry.setCorrection(signInExc.getCorrectionText());
            rcEntry.setDetails(signInExc.getDetailsText());
            if (idNameText != null)
            {
                rcEntry.putObjRef(ApiConsts.KEY_SEC_IDENTITY, idNameText);
            }
            reply.addEntry(rcEntry);
        }

        apiCallAnswerer.answerApiCallRc(reply);
    }

    private void peerSignIn(
        IdentityName idName,
        byte[] password
    )
        throws SignInException, InvalidNameException
    {
        AccessContext peerSignInCtx = idAuthentication.signIn(idName, password);
        try
        {
            AccessContext privCtx = sysCtx.clone();
            privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
            client.setAccessContext(privCtx, peerSignInCtx);
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "Enabling privileges on the system context failed",
                accExc
            );
        }
    }
}
