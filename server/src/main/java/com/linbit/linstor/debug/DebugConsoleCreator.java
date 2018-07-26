package com.linbit.linstor.debug;

import com.linbit.linstor.CommonPeerCtx;
import com.linbit.linstor.ControllerPeerCtx;
import com.linbit.linstor.LinStorModule;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.Privilege;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import java.util.Set;
import javax.inject.Named;
import javax.inject.Provider;

public class DebugConsoleCreator
{
    private final ErrorReporter errorReporter;
    private final Set<CommonDebugCmd> debugCommands;
    private final LinStorScope debugScope;
    private final Provider<TransactionMgr> trnActProvider;

    @Inject
    public DebugConsoleCreator(
        ErrorReporter errorReporterRef,
        LinStorScope debugScopeRef,
        @Named(LinStorModule.TRANS_MGR_GENERATOR) Provider<TransactionMgr> trnActProviderRef,
        Set<CommonDebugCmd> debugCommandsRef
    )
    {
        errorReporter = errorReporterRef;
        debugScope = debugScopeRef;
        trnActProvider = trnActProviderRef;
        debugCommands = debugCommandsRef;
    }

    /**
     * Creates a debug console instance for remote use by a connected peer
     *
     * @param accCtx The access context to authorize this API call
     * @param client Connected peer
     * @return New DebugConsole instance
     * @throws AccessDeniedException If the API call is not authorized
     */
    public DebugConsole createDebugConsole(
        AccessContext accCtx,
        AccessContext debugCtx,
        Peer client
    )
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);
        DebugConsole peerDbgConsole = new DebugConsoleImpl(
            debugCtx, errorReporter, debugScope, trnActProvider, debugCommands
        );
        if (client != null)
        {
            ControllerPeerCtx peerContext = (ControllerPeerCtx) client.getAttachment();
            // Initialize remote debug console
            peerContext.setDebugConsole(peerDbgConsole);
        }

        return peerDbgConsole;
    }

    /**
     * Destroys the debug console instance of a connected peer
     *
     * @param accCtx The access context to authorize this API call
     * @param client Connected peer
     * @throws AccessDeniedException If the API call is not authorized
     */
    public void destroyDebugConsole(AccessContext accCtx, Peer client)
        throws AccessDeniedException
    {
        accCtx.getEffectivePrivs().requirePrivileges(Privilege.PRIV_SYS_ALL);

        CommonPeerCtx peerContext = (CommonPeerCtx) client.getAttachment();
        peerContext.setDebugConsole(null);
    }
}
