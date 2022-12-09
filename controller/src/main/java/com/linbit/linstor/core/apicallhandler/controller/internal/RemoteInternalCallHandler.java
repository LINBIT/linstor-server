package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.repository.RemoteRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class RemoteInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final RemoteRepository remoteRepo;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;

    private final ReadWriteLock remoteMapLock;

    @Inject
    public RemoteInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        RemoteRepository externalFileRepoRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.REMOTE_MAP_LOCK) ReadWriteLock extFileMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        remoteRepo = externalFileRepoRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        remoteMapLock = extFileMapLockRef;
    }

    public void handleRemoteRequest(UUID remoteUuidRef, String remoteNameStr)
    {
        try (
            LockGuard ls = LockGuard.createLocked(
                remoteMapLock.readLock(),
                peer.get().getSerializerLock().readLock()
            )
        )
        {
            RemoteName remoteName = new RemoteName(remoteNameStr, true);

            Peer currentPeer = peer.get();

            long fullSyncId = currentPeer.getFullSyncId();
            long updateId = currentPeer.getNextSerializerId();


            AbsRemote remote = remoteRepo.get(apiCtx, remoteName);
            if (remote != null)
            {
                if (!remote.getUuid().equals(remoteUuidRef))
                {
                    throw new ImplementationError(
                        "Satellite requested remote " + remoteNameStr + " with UUID " + remoteUuidRef +
                            ", but controller's UUID was " + remote.getUuid()
                    );
                }
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_REMOTE)
                        .remote(remote, fullSyncId, updateId)
                        .build()
                );
            }
            else
            {
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_DELETED_REMOTE)
                        .deletedRemote(remoteNameStr, fullSyncId, updateId)
                        .build()
                );
            }
        }
        catch (InvalidNameException invalidNameExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Satellite requested data for invalid name '" + invalidNameExc.invalidName + "'.",
                    invalidNameExc
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            errorReporter.reportError(
                new ImplementationError(
                    "Controller's api context has not enough privileges to gather requested storpool data.",
                    accDeniedExc
                )
            );
        }
    }
}
