package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Identity;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.tasks.PingTask;
import com.linbit.linstor.tasks.ReconnectorTask;
import com.linbit.locks.LockGuardFactory;

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
    private final LockGuardFactory lockGuardFactory;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final ResponseConverter responseConverter;

    @Inject
    public CtrlAuthResponseApiCallHandler(
        ErrorReporter errorReporterRef,
        Provider<Peer> peerProviderRef,
        @SystemContext AccessContext sysCtxRef,
        CtrlFullSyncApiCallHandler ctrlFullSyncApiCallHandlerRef,
        ReconnectorTask reconnectorTaskRef,
        PingTask pingTaskRef,
        LockGuardFactory lockGuardFactoryRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        ResponseConverter responseConverterRef
    )
    {
        errorReporter = errorReporterRef;
        peerProvider = peerProviderRef;
        sysCtx = sysCtxRef;
        ctrlFullSyncApiCallHandler = ctrlFullSyncApiCallHandlerRef;
        reconnectorTask = reconnectorTaskRef;
        pingTask = pingTaskRef;
        lockGuardFactory = lockGuardFactoryRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        responseConverter = responseConverterRef;
    }

    public Flux<ApiCallRc> authResponse(
        Peer peer,
        boolean success,
        ApiCallRcImpl apiCallResponse,
        Long expectedFullSyncId,
        String nodeUname,
        Integer linstorVersionMajor,
        Integer linstorVersionMinor,
        Integer linstorVersionPatch,
        List<ExtToolsInfo> externalToolsInfoList,
        boolean waitForFullSyncAnswerRef
    )
    {
        return scopeRunner.fluxInTransactionalScope(
            "authResponse",
            lockGuardFactory.buildDeferred(LockGuardFactory.LockType.WRITE, LockGuardFactory.LockObj.NODES_MAP),
            () -> authResponseInTransaction(
                peer,
                success,
                apiCallResponse,
                expectedFullSyncId,
                nodeUname,
                linstorVersionMajor,
                linstorVersionMinor,
                linstorVersionPatch,
                externalToolsInfoList,
                waitForFullSyncAnswerRef
            )
        );
    }

    private Flux<ApiCallRc> authResponseInTransaction(
        Peer peer,
        boolean success,
        ApiCallRcImpl apiCallResponse,
        Long expectedFullSyncId,
        String nodeUname,
        Integer linstorVersionMajor,
        Integer linstorVersionMinor,
        Integer linstorVersionPatch,
        List<ExtToolsInfo> externalToolsInfoList,
        boolean waitForFullSyncAnswerRef
    )
    {
        Flux<ApiCallRc> flux;

        if (success)
        {
            if (LinStor.VERSION_INFO_PROVIDER.equalsVersion(
                    linstorVersionMajor,
                    linstorVersionMinor,
                    linstorVersionPatch
                )
            )
            {
                peer.setAuthenticated(true);
                peer.setConnectionStatus(Peer.ConnectionStatus.CONNECTED);
                peer.getExtToolsManager().updateExternalToolsInfo(externalToolsInfoList);

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

                    peer.getNode().getProps(sysCtx).setProp(InternalApiConsts.NODE_UNAME, nodeUname);

                    ctrlTransactionHelper.commit();
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
                catch (InvalidValueException | DatabaseException exc)
                {
                    errorReporter.reportError(exc);
                }
                errorReporter.logDebug("Satellite '" + peer.getNode().getName() + "' authenticated");

                pingTask.add(peer);

                flux = ctrlFullSyncApiCallHandler.sendFullSync(
                    peer.getNode(),
                    expectedFullSyncId,
                    waitForFullSyncAnswerRef
                );

                if (!nodeUname.equalsIgnoreCase(peer.getNode().getName().displayValue))
                {
                    flux = flux.concatWith(Flux.just(
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.INFO_NODE_NAME_MISMATCH,
                            String.format("Linstor node name '%s' and hostname '%s' doesn't match.",
                                peer.getNode().getName().displayValue,
                                nodeUname)
                        ))
                    );
                }
            }
            else
            {
                peer.setConnectionStatus(Peer.ConnectionStatus.VERSION_MISMATCH);
                errorReporter.logError(
                    String.format(
                        "Satellite '%s' version mismatch(v%d.%d.%d).",
                        peer.getNode().getName(),
                        linstorVersionMajor,
                        linstorVersionMinor,
                        linstorVersionPatch
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

            peer.setConnectionStatus(Peer.ConnectionStatus.AUTHENTICATION_ERROR);

            for (RcEntry entry : apiCallResponse.getEntries())
            {
                errorReporter.logError("Satellite authentication error: " + entry.getCause());
            }

            flux = Flux.empty();
        }
        return flux;
    }

}
