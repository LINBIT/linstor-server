package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.inject.Provider;

public class ConfInternalCallHandler
{
    private ErrorReporter errorReporter;
    private final Provider<Peer> peer;

    private final CtrlStltSerializer ctrlStltSrzl;

    private final ReadWriteLock nodesMapLock;

    @Inject
    public ConfInternalCallHandler(
        ErrorReporter errorReporterRef,
        Provider<Peer> peerProviderRef,
        CtrlStltSerializer ctrlStltSrzlRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        peer = peerProviderRef;

        ctrlStltSrzl = ctrlStltSrzlRef;

        nodesMapLock = nodesMapLockRef;
    }

    public void handleControllerRequest(UUID nodeUuid, String nodeNameStr)
    {
        try (LockGuard ls = LockGuard.createLocked(
            nodesMapLock.readLock(),
            peer.get().getSerializerLock().readLock()
        ))
        {
            Peer currentPeer = peer.get();

            currentPeer.sendMessage(
                ctrlStltSrzl
                    .onewayBuilder(InternalApiConsts.API_APPLY_CONTROLLER)
                    .controllerData(currentPeer.getFullSyncId(), currentPeer.getNextSerializerId())
                    .build()
            );
        }
        catch (Exception exc)
        {
            errorReporter.reportError(
                new ImplementationError(exc)
            );
        }
    }
}
