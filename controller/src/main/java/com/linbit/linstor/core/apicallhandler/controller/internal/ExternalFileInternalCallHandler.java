package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.repository.ExternalFileRepository;
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

public class ExternalFileInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ExternalFileRepository externalFileRepo;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;

    private final ReadWriteLock extFileMapLock;

    @Inject
    public ExternalFileInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ExternalFileRepository externalFileRepoRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @Named(CoreModule.EXT_FILE_MAP_LOCK) ReadWriteLock extFileMapLockRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        externalFileRepo = externalFileRepoRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        extFileMapLock = extFileMapLockRef;
    }

    public void handleExternalFileRequest(UUID extFileUuidRef, String extFileNameStr)
    {
        try (
            LockGuard ls = LockGuard.createLocked(
                extFileMapLock.readLock(),
                peer.get().getSerializerLock().readLock()
            )
        )
        {
            ExternalFileName extFileName = new ExternalFileName(extFileNameStr);

            Peer currentPeer = peer.get();

            long fullSyncId = currentPeer.getFullSyncId();
            long updateId = currentPeer.getNextSerializerId();

            ExternalFile extFile = externalFileRepo.get(apiCtx, extFileName);
            if (extFile != null)
            {
                if (!extFile.getUuid().equals(extFileUuidRef))
                {
                    throw new ImplementationError(
                        "Satellite requested extFile " + extFileNameStr + " with UUID " + extFileUuidRef +
                            ", but controller's UUID was " + extFile.getUuid()
                    );
                }
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_EXTERNAL_FILE)
                        .externalFile(extFile, true, fullSyncId, updateId)
                        .build()
                );
            }
            else
            {
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_DELETED_EXTERNAL_FILE)
                        .deletedExternalFile(extFileNameStr, fullSyncId, updateId)
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
