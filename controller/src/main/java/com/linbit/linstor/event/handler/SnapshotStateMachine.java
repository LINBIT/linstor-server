package com.linbit.linstor.event.handler;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotDefinition.SnapshotDfnFlags;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.SnapshotState;
import com.linbit.linstor.event.EventIdentifier;
import com.linbit.linstor.event.ObjectIdentifier;
import com.linbit.linstor.event.controller.SnapshotDeploymentEvent;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

/**
 * Manages snapshot creation.
 * <p>
 * See CtrlSnapshotApiCallHandler#createSnapshot(List, String, String) for a description of the
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
    private final SnapshotDeploymentEvent snapshotDeploymentEvent;

    @Inject
    public SnapshotStateMachine(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CoreModule.ResourceDefinitionMap rscDfnMapRef,
        CtrlStltSerializer ctrlStltSerializerRef,
        SatelliteStateHelper satelliteStateHelperRef,
        SnapshotDeploymentEvent snapshotDeploymentEventRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscDfnMap = rscDfnMapRef;
        ctrlStltSerializer = ctrlStltSerializerRef;
        satelliteStateHelper = satelliteStateHelperRef;
        snapshotDeploymentEvent = snapshotDeploymentEventRef;
    }

    public void stepResourceSnapshots(
        EventIdentifier eventIdentifier,
        ApiCallRc abortionError,
        boolean satelliteDisconnected
    )
    {
        rscDfnMapLock.writeLock().lock();
        try
        {
            ResourceDefinition rscDfn = rscDfnMap.get(eventIdentifier.getResourceName());

            if (rscDfn != null)
            {
                // Shallow copy the collection because elements may be removed from the original collection
                List<SnapshotDefinition> snapshotDefinitions = new ArrayList<>(rscDfn.getSnapshotDfns(apiCtx));
                for (SnapshotDefinition snapshotDefinition : snapshotDefinitions)
                {
                    Snapshot snapshot = snapshotDefinition.getSnapshot(apiCtx, eventIdentifier.getNodeName());
                    if (snapshotDefinition.getInProgress(apiCtx) && snapshot != null)
                    {
                        if (snapshot.getFlags().isSet(apiCtx, Snapshot.SnapshotFlags.DELETE))
                        {
                            processSnapshotDeletion(snapshot);
                        }
                        else
                        {
                            boolean changed;

                            if (abortionError != null)
                            {
                                SnapshotDfnFlags flag = satelliteDisconnected ?
                                    SnapshotDfnFlags.FAILED_DISCONNECT : SnapshotDfnFlags.FAILED_DEPLOYMENT;
                                snapshotDefinition.getFlags().enableFlags(apiCtx, flag);

                                changed = abortSnapshot(snapshotDefinition, abortionError);
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
    private boolean abortSnapshot(SnapshotDefinition snapshotDefinition, ApiCallRc abortionError)
        throws AccessDeniedException, SQLException
    {
        boolean snapshotsChanged = false;
        if (snapshotDefinition.getInProgress(apiCtx))
        {
            errorReporter.logWarning("Aborting snapshot - %s", snapshotDefinition);
            snapshotDefinition.setInCreation(apiCtx, false);
            snapshotsChanged = true;
            closeSnapshotDeploymentEventStream(snapshotDefinition, abortionError);
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

        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots(apiCtx))
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

            if (!snapshot.getSuspendResource(apiCtx))
            {
                allSuspendSet = false;
            }
            if (!snapshot.getTakeSnapshot(apiCtx))
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
                    snapshotDefinition.setInCreation(apiCtx, false);

                    snapshotDefinition.getFlags().enableFlags(apiCtx, SnapshotDfnFlags.SUCCESSFUL);

                    closeSnapshotDeploymentEventStream(snapshotDefinition, null);

                    snapshotsChanged = true;
                }
                else if (allSnapshotTaken && allSuspendSet)
                {
                    errorReporter.logInfo("Resuming after snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots(apiCtx))
                    {
                        snapshot.setSuspendResource(apiCtx, false);
                    }
                    snapshotsChanged = true;
                }
            }
            else
            {
                if (allSuspended)
                {
                    errorReporter.logInfo("Taking snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots(apiCtx))
                    {
                        snapshot.setTakeSnapshot(apiCtx, true);
                    }
                    snapshotsChanged = true;
                }
                else if (!allSuspendSet)
                {
                    errorReporter.logInfo("Suspending for snapshot - %s", snapshotDefinition);
                    for (Snapshot snapshot : snapshotDefinition.getAllSnapshots(apiCtx))
                    {
                        snapshot.setSuspendResource(apiCtx, true);
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
        for (Snapshot snapshot : snapshotDefinition.getAllSnapshots(apiCtx))
        {
            snapshot.getNode().getPeer(apiCtx).sendMessage(
                ctrlStltSerializer
                    .onewayBuilder(InternalApiConsts.API_CHANGED_IN_PROGRESS_SNAPSHOT)
                    .changedSnapshot(
                        snapshotDefinition.getResourceName().displayValue,
                        snapshot.getUuid(),
                        snapshot.getSnapshotName().displayValue
                    )
                    .build()
            );
        }
    }

    private void closeSnapshotDeploymentEventStream(SnapshotDefinition snapshotDefinition, ApiCallRc abortionError)
        throws AccessDeniedException
    {
        ObjectIdentifier objectIdentifier = ObjectIdentifier.snapshotDefinition(
            snapshotDefinition.getResourceName(),
            snapshotDefinition.getName()
        );
        snapshotDeploymentEvent.get().triggerEvent(
            objectIdentifier, determineResponse(snapshotDefinition, abortionError));
        snapshotDeploymentEvent.get().closeStream(objectIdentifier);
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
                snapshotDefinition.getAllSnapshots(apiCtx).isEmpty())
            {
                snapshotDefinition.delete(apiCtx);
            }
        }
    }

    private ApiCallRc determineResponse(SnapshotDefinition snapshotDfn, ApiCallRc abortionError)
        throws AccessDeniedException
    {
        ApiCallRc apiCallRc;

        if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.FAILED_DEPLOYMENT))
        {
            apiCallRc = abortionError;
        }
        else if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.SUCCESSFUL))
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(ApiConsts.CREATED | ApiConsts.MASK_SNAPSHOT);
            entry.setMessage(String.format("Snapshot '%s' of resource '%s' successfully taken.",
                snapshotDfn.getName().displayValue,
                snapshotDfn.getResourceName().displayValue));
            ApiCallRcImpl successRc = new ApiCallRcImpl();
            successRc.addEntry(entry);

            apiCallRc = successRc;
        }
        else if (snapshotDfn.getFlags().isSet(apiCtx, SnapshotDefinition.SnapshotDfnFlags.FAILED_DISCONNECT))
        {
            ApiCallRcImpl.ApiCallRcEntry entry = new ApiCallRcImpl.ApiCallRcEntry();
            entry.setReturnCode(ApiConsts.FAIL_NOT_CONNECTED);
            entry.setMessage(String.format(
                "Snapshot '%s' of resource '%s' failed due to satellite disconnection.",
                snapshotDfn.getName().displayValue,
                snapshotDfn.getResourceName().displayValue
            ));
            ApiCallRcImpl failedRc = new ApiCallRcImpl();
            failedRc.addEntry(entry);

            apiCallRc = failedRc;
        }
        else
        {
            // Still in progress - empty response
            apiCallRc = new ApiCallRcImpl();
        }

        return apiCallRc;
    }
}
