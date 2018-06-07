package com.linbit.linstor.event.handler;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinition.SnapshotDfnFlags;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

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
                for (SnapshotDefinition snapshotDefinition : rscDfn.getInProgressSnapshotDfns(apiCtx))
                {
                    Snapshot snapshot = snapshotDefinition.getSnapshot(eventIdentifier.getNodeName());
                    if (snapshot != null)
                    {
                        if (snapshot.getFlags().isSet(apiCtx, Snapshot.SnapshotFlags.DELETE))
                        {
                            processSnapshotDeletion(snapshot);
                        }
                        else
                        {
                            boolean changed;

                            if (abort)
                            {
                                SnapshotDfnFlags flag = satelliteDisconnected ?
                                    SnapshotDfnFlags.FAILED_DISCONNECT : SnapshotDfnFlags.FAILED_DEPLOYMENT;
                                snapshotDefinition.getFlags().enableFlags(apiCtx, flag);

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
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Insufficient privileges to access resource for taking snapshot", exc);
        }
        catch (SQLException exc)
        {
            throw new ImplementationError("Failed to continue processing snapshot", exc);
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
        boolean snapshotsChanged = false;
        if (snapshotDefinition.getResourceDefinition().isSnapshotInProgress(snapshotDefinition.getName()))
        {
            errorReporter.logWarning("Aborting snapshot - %s", snapshotDefinition);
            snapshotDefinition.getResourceDefinition().markSnapshotInProgress(snapshotDefinition.getName(), false);
            snapshotsChanged = true;
            closeSnapshotDeploymentEventStream(snapshotDefinition);
        }

        return snapshotsChanged;
    }

    /**
     * @return true if the in-progress-snapshots have been changed.
     */
    private boolean processSnapshot(SnapshotDefinition snapshotDefinition)
        throws AccessDeniedException, SQLException
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

        boolean snapshotsChanged = false;
        if (allSnapshotReceived)
        {
            if (allTakeSnapshotSet)
            {
                if (noneSuspended)
                {
                    errorReporter.logInfo("Finished snapshot - %s", snapshotDefinition);
                    snapshotDefinition.getResourceDefinition().markSnapshotInProgress(
                        snapshotDefinition.getName(), false);

                    snapshotDefinition.getFlags().enableFlags(apiCtx, SnapshotDfnFlags.SUCCESSFUL);

                    closeSnapshotDeploymentEventStream(snapshotDefinition);

                    snapshotsChanged = true;
                }
                else if (allSnapshotTaken && allSuspendSet)
                {
                    errorReporter.logInfo("Resuming after snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
                    {
                        snapshot.setSuspendResource(false);
                    }
                    snapshotsChanged = true;
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
                    snapshotsChanged = true;
                }
                else if (!allSuspendSet)
                {
                    errorReporter.logInfo("Suspending for snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
                    {
                        snapshot.setSuspendResource(true);
                    }
                    snapshotsChanged = true;
                }
            }
        }

        return snapshotsChanged;
    }

    private SnapshotState getSnapshotState(Snapshot snapshot)
    {
        return satelliteStateHelper.withSatelliteState(
            snapshot.getNodeName(),
            satelliteState -> satelliteState.getSnapshotState(
                snapshot.getResourceName(),
                snapshot.getSnapshotName()
            ),
            null
        );
    }

    private void updateSatellites(SnapshotDefinition snapshotDefinition)
        throws AccessDeniedException
    {
        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots())
        {
            snapshot.getNode().getPeer(apiCtx).sendMessage(
                ctrlStltSerializer
                    .builder(InternalApiConsts.API_CHANGED_IN_PROGRESS_SNAPSHOT, 0)
                    .changedSnapshot(
                        snapshotDefinition.getResourceName().displayValue,
                        snapshot.getUuid(),
                        snapshot.getSnapshotName().displayValue
                    )
                    .build()
            );
        }
    }

    private void closeSnapshotDeploymentEventStream(SnapshotDefinition snapshotDefinition)
    {
        eventBroker.closeEventStream(EventIdentifier.snapshotDefinition(
            ApiConsts.EVENT_SNAPSHOT_DEPLOYMENT,
            snapshotDefinition.getResourceName(),
            snapshotDefinition.getName()
        ));
    }

    private void processSnapshotDeletion(Snapshot snapshot)
        throws AccessDeniedException, SQLException
    {
        SnapshotState snapshotState = getSnapshotState(snapshot);

        if (snapshotState != null && snapshotState.isSnapshotDeleted())
        {
            deleteSnapshot(snapshot);
        }
    }

    private void deleteSnapshot(Snapshot snapshot)
        throws AccessDeniedException, SQLException
    {
        if (snapshot.getFlags().isSet(apiCtx, Snapshot.SnapshotFlags.DELETE))
        {
            SnapshotDefinition snapshotDefinition = snapshot.getSnapshotDefinition();

            snapshot.delete(apiCtx);

            if (snapshotDefinition.getFlags().isSet(apiCtx, SnapshotDfnFlags.DELETE) &&
                snapshotDefinition.getAllSnapshots().isEmpty())
            {
                snapshotDefinition.delete(apiCtx);
            }
        }
    }
}
