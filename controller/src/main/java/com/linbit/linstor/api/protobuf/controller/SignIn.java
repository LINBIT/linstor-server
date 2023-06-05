package com.linbit.linstor.api.protobuf.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.ControllerDatabase;
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
import com.linbit.linstor.proto.requests.MsgSignInOuterClass;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.CtrlAuthentication;
import com.linbit.linstor.security.IdentityName;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.security.SignInException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;

@ProtobufApiCall(
    name = ApiConsts.API_SIGN_IN,
    description = "Performs a sign-in with the specified credentials",
    requiresAuth = false,
    transactional = false
)
@Singleton
public class SignIn implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final CtrlAuthentication<ControllerDatabase> idAuthentication;
    private final ApiCallAnswerer apiCallAnswerer;
    private final AccessContext sysCtx;
    private final Provider<AccessContext> clientCtxProvider;
    private final Provider<Peer> clientProvider;

    @Inject
    public SignIn(
        ErrorReporter errorReporterRef,
        CtrlAuthentication<ControllerDatabase> idAuthenticationRef,
        ApiCallAnswerer apiCallAnswererRef,
        @SystemContext AccessContext sysCtxRef,
        @PeerContext Provider<AccessContext> clientCtxProviderRef,
        Provider<Peer> clientProviderRef
    )
    {
        errorReporter = errorReporterRef;
        idAuthentication = idAuthenticationRef;
        apiCallAnswerer = apiCallAnswererRef;
        sysCtx = sysCtxRef;
        clientCtxProvider = clientCtxProviderRef;
        clientProvider = clientProviderRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
    {
        ApiCallRcImpl reply = new ApiCallRcImpl();
        String idNameText = null;
        AccessContext clientCtx = clientCtxProvider.get();
        try
        {
            MsgSignInOuterClass.MsgSignIn signInData = MsgSignInOuterClass.MsgSignIn.parseDelimitedFrom(msgDataIn);
            idNameText = signInData.getIdName();
            byte[] password = signInData.getPasswordBytes().toByteArray();

            IdentityName idName = new IdentityName(idNameText);

            peerSignIn(idName, password);

            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setMessage("Sign-in successful");
            rcEntry.setReturnCode(ApiConsts.SUCCESS_SIGN_IN);
            rcEntry.putObjRef(ApiConsts.KEY_SEC_IDENTITY, clientCtx.subjectId.name.displayValue);
            rcEntry.putObjRef(ApiConsts.KEY_SEC_ROLE, clientCtx.subjectRole.name.displayValue);
            rcEntry.putObjRef(ApiConsts.KEY_SEC_DOMAIN, clientCtx.subjectDomain.name.displayValue);
            reply.addEntry(rcEntry);
        }
        catch (IOException ioExc)
        {
            String reportId = errorReporter.reportError(ioExc, clientCtx, clientProvider.get(), "Sign-in");
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.FAIL_SIGN_IN);
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
            rcEntry.addErrorId(reportId);
            reply.addEntry(rcEntry);
        }
        catch (InvalidNameException nameExc)
        {
            ApiCallRcEntry rcEntry = new ApiCallRcEntry();
            rcEntry.setReturnCode(ApiConsts.FAIL_SIGN_IN);
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
            rcEntry.setReturnCode(ApiConsts.FAIL_SIGN_IN);
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
            clientProvider.get().setAccessContext(privCtx, peerSignInCtx);
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
