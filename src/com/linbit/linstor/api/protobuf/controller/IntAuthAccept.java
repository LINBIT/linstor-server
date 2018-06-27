package com.linbit.linstor.api.protobuf.controller;

import com.linbit.ImplementationError;
import javax.inject.Inject;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCall;
import com.linbit.linstor.api.protobuf.ProtobufApiCall;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.MsgIntAuthSuccessOuterClass.MsgIntAuthSuccess;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Privilege;

import java.io.IOException;
import java.io.InputStream;
import org.slf4j.event.Level;

@ProtobufApiCall(
    name = InternalApiConsts.API_AUTH_ACCEPT,
    description = "Called by the satellite to indicate that controller authentication succeeded",
    requiresAuth = false
)
public class IntAuthAccept implements ApiCall
{
    private final ErrorReporter errorReporter;
    private final CtrlApiCallHandler apiCallHandler;
    private final Peer client;
    private final AccessContext sysCtx;

    @Inject
    public IntAuthAccept(
        ErrorReporter errorReporterRef,
        CtrlApiCallHandler apiCallHandlerRef,
        Peer clientRef,
        @SystemContext AccessContext sysCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCallHandler = apiCallHandlerRef;
        client = clientRef;
        sysCtx = sysCtxRef;
    }

    @Override
    public void execute(InputStream msgDataIn)
        throws IOException
    {
        MsgIntAuthSuccess msgIntAuthSuccess = MsgIntAuthSuccess.parseDelimitedFrom(msgDataIn);
        long expectedFullSyncId = msgIntAuthSuccess.getExpectedFullSyncId();

        if (LinStor.VERSION_INFO_PROVIDER.equalsVersion(
            msgIntAuthSuccess.getVersionMajor(),
            msgIntAuthSuccess.getVersionMinor(),
            msgIntAuthSuccess.getVersionPatch()))
        {
            client.setAuthenticated(true);
            client.setConnectionStatus(Peer.ConnectionStatus.CONNECTED);

            // Set the satellite's access context
            // Certain APIs called by the satellite are executed with a privileged access context by the controller,
            // while the access context of the peer connection itself remains unprivileged
            AccessContext curCtx = client.getAccessContext();
            AccessContext privCtx = sysCtx.clone();
            try
            {
                privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                // FIXME In the absence of any means of identification, assume the system identity for the peer.
                // Set the SYSTEM identity on the Satellite's access context
                AccessContext newCtx = privCtx.impersonate(Identity.SYSTEM_ID, curCtx.subjectRole, curCtx.subjectDomain);
                // Disable all privileges on the Satellite's access context permanently
                newCtx.getLimitPrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);
                client.setAccessContext(privCtx, newCtx);
            }
            catch (AccessDeniedException accExc)
            {
                errorReporter.reportError(
                    Level.ERROR,
                    new ImplementationError(
                        "Creation of an access context for a Satellite by the " + privCtx.subjectRole.name.displayValue +
                            " role failed",
                        accExc
                    )
                );
            }
            errorReporter.logDebug("Satellite '" + client.getNode().getName() + "' authenticated");

            apiCallHandler.sendFullSync(expectedFullSyncId);
        }
        else
        {
            client.setConnectionStatus(Peer.ConnectionStatus.VERSION_MISMATCH);
            errorReporter.logError(
                String.format(
                    "Satellite '%s' version mismatch(v%d.%d.%d).",
                    client.getNode().getName(),
                    msgIntAuthSuccess.getVersionMajor(),
                    msgIntAuthSuccess.getVersionMinor(),
                    msgIntAuthSuccess.getVersionPatch()
                )
            );
            client.closeConnection();
        }
    }
}
