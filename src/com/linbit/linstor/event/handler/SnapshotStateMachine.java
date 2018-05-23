package com.linbit.linstor.event.handler;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotName;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CtrlSnapshotApiCallHandler;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.EventBroker;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.generator.SatelliteStateHelper;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages snapshot creation.
 * <p>
 * See {@link CtrlSnapshotApiCallHandler#createSnapshot(java.lang.String, java.lang.String)} for a description of the
 * snapshot creation process.
 */
@Singleton
public class SnapshotStateMachine
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final ReadWriteLock rscDfnMapLock;
    private final CoreModule.ResourceDefinitionMap rscDfnMap;
    private final CtrlStltSerializer ctrlStltSerializer;
    private final SatelliteStateHelper satelliteStateHelper;
    private final EventBroker eventBroker;

    @Inject
    public SnapshotStateMachine(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        SatelliteStateHelper satelliteStateHelperRef,
        EventBroker eventBrokerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscDfnMap = rscDfnMapRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        satelliteStateHelper = satelliteStateHelperRef;
        eventBroker = eventBrokerRef;
    }

    public void stepResourceSnapshots(EventIdentifier eventIdentifier, boolean abort, boolean satelliteDisconnected)
    {
        rscDfnMapLock.writeLock().lock();
        try
        {
            ResourceDefinition rscDfn = rscDfnMap.get(eventIdentifier.getResourceName());

            if (rscDfn != null)
            {
                Resource rsc = rscDfn.getResource(apiCtx, eventIdentifier.getNodeName());

                if (rsc != null)
                {
                    List<SnapshotDefinition> snapshotDefinitions = rsc.getInProgressSnapshots().stream()
                        .map(Snapshot::getSnapshotDefinition)
                        .collect(Collectors.toList());
                    for (SnapshotDefinition snapshotDefinition : snapshotDefinitions)
                    {
                        boolean changed;

                        if (satelliteDisconnected)
                        {
                            snapshotDefinition.setFailedDueToDisconnect(true);
                        }

                        if (abort)
                        {
                            changed = abortSnapshot(snapshotDefinition);
                        }
                        else
                        {
                            changed = processSnapshot(snapshotDefinition);
                        }

                        if (changed)
                        {
                            updateSatellites(snapshotDefinition);
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Insufficient privileges to access resource for taking snapshot", exc);
        }
        finally
        {
            rscDfnMapLock.writeLock().unlock();
        }
    }

    /**
     * @return true if the in-progress-snapshots have been changed.
     */
    private boolean abortSnapshot(SnapshotDefinition snapshotDefinition)
        throws AccessDeniedException
    {
        boolean resourcesChanged = false;
        errorReporter.logWarning("Aborting snapshot - %s", snapshotDefinition);
        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
        {
            Resource resource = snapshotDefinition.getResourceDefinition()
                .getResource(apiCtx, snapshot.getNode().getName());

            SnapshotName snapshotName = snapshot.getSnapshotDefinition().getName();
            Snapshot inProgressSnapshot = resource.getInProgressSnapshot(snapshotName);
            if (inProgressSnapshot != null)
            {
                resource.removeInProgressSnapshot(snapshotName);
                resourcesChanged = true;
            }
        }

        closeSnapshotDeploymentEventStream(snapshotDefinition);

        return resourcesChanged;
    }

    /**
     * @return true if the in-progress-snapshots have been changed.
     */
    private boolean processSnapshot(SnapshotDefinition snapshotDefinition)
        throws AccessDeniedException
    {
        boolean allSnapshotReceived = true;
        boolean allSuspended = true;
        boolean noneSuspended = true;
        boolean allSnapshotTaken = true;
        boolean allSuspendSet = true;
        boolean allTakeSnapshotSet = true;

        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
        {
            SnapshotState snapshotState = getSnapshotState(snapshot);
            if (snapshotState == null)
            {
                allSnapshotReceived = false;
            }
            else
            {
                if (snapshotState.isSuspended())
                {
                    noneSuspended = false;
                }
                else
                {
                    allSuspended = false;
                }
                if (!snapshotState.isSnapshotTaken())
                {
                    allSnapshotTaken = false;
                }
            }

            if (!snapshot.getSuspendResource())
            {
                allSuspendSet = false;
            }
            if (!snapshot.getTakeSnapshot())
            {
                allTakeSnapshotSet = false;
            }
        }

        boolean resourcesChanged = false;
        if (allSnapshotReceived)
        {
            if (allTakeSnapshotSet)
            {
                if (noneSuspended)
                {
                    errorReporter.logInfo("Finished snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
                    {
                        Resource resource = snapshotDefinition.getResourceDefinition()
                            .getResource(apiCtx, snapshot.getNode().getName());

                        resource.removeInProgressSnapshot(snapshot.getSnapshotDefinition().getName());
                    }

                    snapshotDefinition.setSuccessfullyTaken(true);

                    closeSnapshotDeploymentEventStream(snapshotDefinition);

                    resourcesChanged = true;
                }
                else if (allSnapshotTaken && allSuspendSet)
                {
                    errorReporter.logInfo("Resuming after snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
                    {
                        snapshot.setSuspendResource(false);
                    }
                    resourcesChanged = true;
                }
            }
            else
            {
                if (allSuspended)
                {
                    errorReporter.logInfo("Taking snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
                    {
                        snapshot.setTakeSnapshot(true);
                    }
                    resourcesChanged = true;
                }
                else if (!allSuspendSet)
                {
                    errorReporter.logInfo("Suspending for snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
                    {
                        snapshot.setSuspendResource(true);
                    }
                    resourcesChanged = true;
                }
            }
        }

        return resourcesChanged;
    }

    private SnapshotState getSnapshotState(Snapshot snapshot)
    {
        return satelliteStateHelper.withSatelliteState(
            snapshot.getNode().getName(),
            satelliteState -> satelliteState.getSnapshotState(
                snapshot.getSnapshotDefinition().getResourceDefinition().getName(),
                snapshot.getSnapshotDefinition().getName()
            ),
            null
        );
    }

    private void updateSatellites(SnapshotDefinition snapshotDefinition)
        throws AccessDeniedException
    {
        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
        {
            Resource resource = snapshotDefinition.getResourceDefinition()
                .getResource(apiCtx, snapshot.getNode().getName());

            snapshot.getNode().getPeer(apiCtx).sendMessage(
                ctrlStltSerializer
                    .builder(InternalApiConsts.API_CHANGED_RSC, 0)
                    .changedResource(
                        resource.getUuid(),
                        resource.getDefinition().getName().displayValue
                    )
                    .build()
            );
        }
    }

    private void closeSnapshotDeploymentEventStream(SnapshotDefinition snapshotDefinition)
    {
        eventBroker.closeEventStream(EventIdentifier.snapshotDefinition(
            ApiConsts.EVENT_SNAPSHOT_DEPLOYMENT,
            snapshotDefinition.getResourceDefinition().getName(),
            snapshotDefinition.getName()
        ));
    }
}
