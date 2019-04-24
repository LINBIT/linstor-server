package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;

import org.slf4j.event.Level;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlAuthResponseApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final Provider<Peer> peerProvider;
    private final AccessContext sysCtx;

    private final CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandler;
    private final ReconnectorTask reconnectorTask;
    private final PingTask pingTask;

    @Inject
    public CtrlAuthResponseApiCallHandler(
        ErrorReporter errorReporterRef,
        Provider<Peer> peerProviderRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandlerRef,
        ReconnectorTask reconnectorTaskRef,
        PingTask pingTaskRef
    )
    {
        errorReporter = errorReporterRef;
        peerProvider = peerProviderRef;
        sysCtx = sysCtxRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        reconnectorTask = reconnectorTaskRef;
        pingTask = pingTaskRef;
    }

    public Flux<ApiCallRc> authResponse(
        Peer peer,
        boolean success,
        ApiCallRcImpl apiCallResponse,
        Long expectedFullSyncId,
        Integer versionMajor,
        Integer versionMinor,
        Integer versionPatch,
        List<DeviceLayerKind> supportedLayers,
        List<DeviceProviderKind> supportedProviders,
        boolean waitForFullSyncAnswerRef
    )
    {
        Flux<ApiCallRc> flux;

        if (success)
        {
            if (LinStor.VERSION_INFO_PROVIDER.equalsVersion(
                versionMajor,
                versionMinor,
                versionPatch)
            )
            {
                peer.setAuthenticated(true);
                peer.setConnectionStatus(Peer.ConnectionStatus.CONNECTED);
                peer.setSupportedLayers(supportedLayers);
                peer.setSupportedProviders(supportedProviders);

                // Set the satellite's access context
                // Certain APIs called by the satellite are executed with a privileged access context by the controller,
                // while the access context of the peer connection itself remains unprivileged
                AccessContext curCtx = peer.getAccessContext();
                AccessContext privCtx = sysCtx.clone();
                try
                {
                    privCtx.getEffectivePrivs().enablePrivileges(Privilege.PRIV_SYS_ALL);
                    // FIXME In the absence of any means of identification, assume the system identity for the peer.
                    // Set the SYSTEM identity on the Satellite's access context
                    AccessContext newCtx = privCtx.impersonate(
                        Identity.SYSTEM_ID, curCtx.subjectRole, curCtx.subjectDomain
                    );
                    // Disable all privileges on the Satellite's access context permanently
                    newCtx.getLimitPrivs().disablePrivileges(Privilege.PRIV_SYS_ALL);
                    peer.setAccessContext(privCtx, newCtx);
                }
                catch (AccessDeniedException accExc)
                {
                    errorReporter.reportError(
                        Level.ERROR,
                        new ImplementationError(
                            "Creation of an access context for a Satellite by the " +
                                privCtx.subjectRole.name.displayValue + " role failed",
                            accExc
                        )
                    );
                }
                errorReporter.logDebug("Satellite '" + peer.getNode().getName() + "' authenticated");

                pingTask.add(peer);

                flux = ctrlFullSyncApiCallHandler.sendFullSync(
                    peer.getNode(),
                    expectedFullSyncId,
                    waitForFullSyncAnswerRef
                );
            }
            else
            {
                peer.setConnectionStatus(Peer.ConnectionStatus.VERSION_MISMATCH);
                errorReporter.logError(
                    String.format(
                        "Satellite '%s' version mismatch(v%d.%d.%d).",
                        peer.getNode().getName(),
                        versionMajor,
                        versionMinor,
                        versionPatch
                    )
                );
                peer.closeConnection();

                reconnectorTask.add(peer, false);

                flux = Flux.empty();
            }
        }
        else
        {
            peer.setAuthenticated(false);

            for (RcEntry entry : apiCallResponse.getEntries())
            {
                if (entry.getReturnCode() == InternalApiConsts.API_AUTH_ERROR_HOST_MISMATCH)
                {
                    peer.setConnectionStatus(Peer.ConnectionStatus.HOSTNAME_MISMATCH);
                }
                else
                {
                    peer.setConnectionStatus(Peer.ConnectionStatus.AUTHENTICATION_ERROR);
                }
                errorReporter.logError("Satellite authentication error: " + entry.getCause());
            }

            flux = Flux.empty();
        }
        return flux;
    }

}
