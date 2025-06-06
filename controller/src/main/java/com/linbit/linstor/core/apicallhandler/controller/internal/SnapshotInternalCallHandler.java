package com.linbit.linstor.core.apicallhandler.controller.internal;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.controller.mgr.SnapshotRollbackManager;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.locks.LockGuard;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;

public class SnapshotInternalCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final LockGuardFactory lockGuardFactory;
    private final SnapshotRollbackManager snapRollbackMgr;

    private final ReadWriteLock rscDfnMapLock;

    @Inject
    public SnapshotInternalCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        LockGuardFactory lockGuardFactoryRef,
        SnapshotRollbackManager snapRollbackMgrRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        rscDfnMapLock = rscDfnMapLockRef;
        lockGuardFactory = lockGuardFactoryRef;
        snapRollbackMgr = snapRollbackMgrRef;
    }

    public void handleSnapshotRequest(String resourceNameStr, UUID snapshotUuid, String snapshotNameStr)
    {
        try (
            LockGuard ls = LockGuard.createLocked(
                rscDfnMapLock.readLock(),
                peer.get().getSerializerLock().readLock()
            )
        )
        {
            ResourceName resourceName = new ResourceName(resourceNameStr);
            SnapshotName snapshotName = new SnapshotName(snapshotNameStr);

            Peer currentPeer = peer.get();

            Snapshot snapshot = null;
            ResourceDefinition rscDefinition = resourceDefinitionRepository.get(apiCtx, resourceName);
            if (rscDefinition != null)
            {
                SnapshotDefinition snapshotDfn = rscDefinition.getSnapshotDfn(apiCtx, snapshotName);
                if (snapshotDfn != null /* && snapshotDfn.getInProgress(apiCtx) */)
                {
                    snapshot = snapshotDfn.getSnapshot(peerAccCtx.get(), currentPeer.getNode().getName());
                }
            }

            long fullSyncId = currentPeer.getFullSyncId();
            long updateId = currentPeer.getNextSerializerId();
            if (snapshot != null)
            {
                // TODO: check if the snapshot has the same uuid as snapshotUuid
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT)
                        .snapshot(snapshot, fullSyncId, updateId)
                        .build()
                );
            }
            else
            {
                currentPeer.sendMessage(
                    ctrlStltSerializer
                        .onewayBuilder(InternalApiConsts.API_APPLY_IN_PROGRESS_SNAPSHOT_ENDED)
                        .endedSnapshot(resourceNameStr, snapshotNameStr, fullSyncId, updateId)
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

    public void handleSnapshotRollbackResult(String nodeNameRef, String rscNameRef, boolean successRef)
    {
        try (LockGuard lg = lockGuardFactory.build(LockType.READ, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP))
        {
            NodeName nodeName = new NodeName(nodeNameRef);
            ResourceName rscName = new ResourceName(rscNameRef);
            snapRollbackMgr.handle(nodeName, rscName, successRef);
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
    }
}
