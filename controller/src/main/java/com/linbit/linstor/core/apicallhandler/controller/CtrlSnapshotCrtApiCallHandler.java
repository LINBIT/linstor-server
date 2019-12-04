package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionControllerFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotVlmDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.getSnapshotVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotApiCallHandler.makeSnapshotContext;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmListApiCallHandler.getVlmDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;
import static com.linbit.linstor.utils.layer.LayerVlmUtils.getStorPoolMap;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@Singleton
public class CtrlSnapshotCrtApiCallHandler
{
    private final AccessContext apiCtx;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlSnapshotHelper ctrlSnapshotHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final SnapshotDefinitionControllerFactory snapshotDefinitionFactory;
    private final SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactory;
    private final SnapshotControllerFactory snapshotFactory;
    private final SnapshotVolumeControllerFactory snapshotVolumeControllerFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlRscLayerDataFactory layerStackHelper;
    private final CtrlPropsHelper ctrlPropsHelper;

    @Inject
    public CtrlSnapshotCrtApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SnapshotDefinitionControllerFactory snapshotDefinitionFactoryRef,
        SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactoryRef,
        SnapshotControllerFactory snapshotFactoryRef,
        SnapshotVolumeControllerFactory snapshotVolumeControllerFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscLayerDataFactory layerStackHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotDefinitionFactory = snapshotDefinitionFactoryRef;
        snapshotVolumeDefinitionControllerFactory = snapshotVolumeDefinitionControllerFactoryRef;
        snapshotFactory = snapshotFactoryRef;
        snapshotVolumeControllerFactory = snapshotVolumeControllerFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        layerStackHelper = layerStackHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
    }

    /**
     * Create a snapshot of a resource.
     * <p>
     * Snapshots are created in a multi-stage process:
     * <ol>
     *     <li>Add the snapshot objects (definition and instances), marked with the suspend flag</li>
     *     <li>When all resources are suspended, send out a snapshot request</li>
     *     <li>When all snapshots have been created, mark the resource as resuming by removing the suspend flag</li>
     *     <li>When all resources have been resumed, remove the in-progress snapshots</li>
     * </ol>
     */
    public Flux<ApiCallRc> createSnapshot(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        ResponseContext context = makeSnapshotContext(
            ApiOperation.makeCreateOperation(),
            nodeNameStrs,
            rscNameStr,
            snapshotNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Create snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> createSnapshotInTransaction(nodeNameStrs, rscNameStr, snapshotNameStr)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> createSnapshotInTransaction(
        List<String> nodeNameStrs,
        String rscNameStr,
        String snapshotNameStr
    )
    {
        final ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);
        final ResourceName rscName = rscDfn.getName();

        SnapshotName snapshotName = LinstorParsingUtils.asSnapshotName(snapshotNameStr);
        SnapshotDefinition snapshotDfn = createSnapshotDfnData(
            rscDfn,
            snapshotName,
            new SnapshotDefinition.Flags[] {}
        );
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(rscDfn),
            ctrlPropsHelper.getProps(snapshotDfn)
        );

        ensureSnapshotsViable(rscDfn);

        setInCreation(snapshotDfn);

        Iterator<VolumeDefinition> vlmDfnIterator = iterateVolumeDfn(rscDfn);
        List<SnapshotVolumeDefinition> snapshotVolumeDefinitions = new ArrayList<>();
        while (vlmDfnIterator.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIterator.next();

            SnapshotVolumeDefinition snapshotVlmDfn = createSnapshotVlmDfnData(snapshotDfn, vlmDfn);
            snapshotVolumeDefinitions.add(snapshotVlmDfn);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(vlmDfn),
                ctrlPropsHelper.getProps(snapshotVlmDfn)
            );
        }

        boolean resourceFound = false;
        if (nodeNameStrs.isEmpty())
        {
            Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
            while (rscIterator.hasNext())
            {
                Resource rsc = rscIterator.next();

                if (!isDisklessPrivileged(rsc))
                {
                    createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                    resourceFound = true;
                }
            }
        }
        else
        {
            for (String nodeNameStr : nodeNameStrs)
            {
                Resource rsc = ctrlApiDataLoader.loadRsc(rscDfn, nodeNameStr, true);

                if (isDisklessPrivileged(rsc))
                {
                    throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                        "Cannot create snapshot from diskless resource on node '" + nodeNameStr + "'"
                    ));
                }
                createSnapshotOnNode(snapshotDfn, snapshotVolumeDefinitions, rsc);
                resourceFound = true;
            }
        }

        if (!resourceFound)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_NOT_FOUND_RSC, "No resources found for snapshotting"
            ));
        }

        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIterator.hasNext())
        {
            Resource rsc = rscIterator.next();
            setSuspend(rsc, true);
        }

        ctrlTransactionHelper.commit();

        ApiCallRcImpl responses = new ApiCallRcImpl();

        responses.addEntry(ApiSuccessUtils.defaultRegisteredEntry(
            snapshotDfn.getUuid(), getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr)
        ));

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
                .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), Flux.empty()))
                .transform(updateResponses -> CtrlResponseUtils.combineResponses(
                    updateResponses,
                    rscName,
                    "Suspended IO of {1} on {0} for snapshot"
                ));

        return Flux
            .<ApiCallRc>just(responses)
            .concatWith(satelliteUpdateResponses)
            .concatWith(takeSnapshot(rscName, snapshotName))
            .onErrorResume(exception -> abortSnapshot(rscName, snapshotName, exception))
            .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());
    }

    private Flux<ApiCallRc> abortSnapshot(
        ResourceName rscName,
        SnapshotName snapshotName,
        Throwable exception
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Abort taking snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> abortSnapshotInTransaction(rscName, snapshotName, exception)
            );
    }

    private Flux<ApiCallRc> abortSnapshotInTransaction(
        ResourceName rscName,
        SnapshotName snapshotName,
        Throwable exception
    )
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        SnapshotDefinition.Flags flag = exception instanceof CtrlResponseUtils.DelayedApiRcException &&
            isFailNotConnected((CtrlResponseUtils.DelayedApiRcException) exception) ?
                SnapshotDefinition.Flags.FAILED_DISCONNECT : SnapshotDefinition.Flags.FAILED_DEPLOYMENT;

        enableFlagPrivileged(snapshotDfn, flag);
        unsetInCreationPrivileged(snapshotDfn);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedCannotAbort())
                .transform(responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    rscName,
                    "Aborted snapshot of {1} on {0}"
                ))
                .onErrorResume(CtrlResponseUtils.DelayedApiRcException.class, ignored -> Flux.empty());

        return satelliteUpdateResponses
            .concatWith(Flux.error(exception));
    }

    private Flux<ApiCallRc> takeSnapshot(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Take snapshot",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> takeSnapshotInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> takeSnapshotInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            setTakeSnapshotPrivileged(snapshot, true);
        }

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
                .transform(responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    rscName,
                    "Took snapshot of {1} on {0}"
                ));

        return satelliteUpdateResponses
            .concatWith(resumeResource(rscName, snapshotName));
    }

    private Flux<ApiCallRc> resumeResource(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resume resource",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resumeResourceInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> resumeResourceInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            unsetSuspendResourcePrivileged(snapshot);
        }

        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
        resumeIoPrivileged(rscDfn);

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
                .concatWith(ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), Flux.empty()))
                .transform(responses -> CtrlResponseUtils.combineResponses(
                    responses,
                    rscName,
                    "Resumed IO of {1} on {0} after snapshot"
                ));

        return satelliteUpdateResponses
            .concatWith(removeInProgressSnapshots(rscName, snapshotName));
    }

    private Flux<ApiCallRc> removeInProgressSnapshots(ResourceName rscName, SnapshotName snapshotName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Clean up in-progress snapshots",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> removeInProgressSnapshotsInTransaction(rscName, snapshotName)
            );
    }

    private Flux<ApiCallRc> removeInProgressSnapshotsInTransaction(ResourceName rscName, SnapshotName snapshotName)
    {
        SnapshotDefinition snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        unsetInCreationPrivileged(snapshotDfn);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            setTakeSnapshotPrivileged(snapshot, false);
        }

        enableFlagPrivileged(snapshotDfn, SnapshotDefinition.Flags.SUCCESSFUL);

        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
            // ensure that the individual node update fluxes are subscribed to, but ignore responses from cleanup
            .flatMap(Tuple2::getT2).thenMany(Flux.empty());
    }

    private void createSnapshotOnNode(
        SnapshotDefinition snapshotDfn,
        Collection<SnapshotVolumeDefinition> snapshotVolumeDefinitions,
        Resource rsc
    )
    {
        Snapshot snapshot = createSnapshot(snapshotDfn, rsc);
        ctrlPropsHelper.copy(
            ctrlPropsHelper.getProps(rsc),
            ctrlPropsHelper.getProps(snapshot)
        );

        setSuspend(snapshot);

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
        {
            SnapshotVolume snapVlm = createSnapshotVolume(rsc, snapshot, snapshotVolumeDefinition);

            ctrlPropsHelper.copy(
                ctrlPropsHelper.getProps(rsc.getVolume(snapshotVolumeDefinition.getVolumeNumber())),
                ctrlPropsHelper.getProps(snapVlm)
            );
        }
    }

    private void ensureSnapshotsViable(ResourceDefinition rscDfn)
    {
        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIterator.hasNext())
        {
            Resource currentRsc = rscIterator.next();
            ensureDriversSupportSnapshots(currentRsc);
            ctrlSnapshotHelper.ensureSatelliteConnected(
                currentRsc,
                "Snapshots cannot be created when the corresponding satellites are not connected."
            );
        }
    }

    private void ensureDriversSupportSnapshots(Resource rsc)
    {
        try
        {
            if (!isDisklessPrivileged(rsc))
            {
                Iterator<Volume> vlmIterator = rsc.iterateVolumes();
                while (vlmIterator.hasNext())
                {
                    Volume vlm = vlmIterator.next();
                    Map<String, StorPool> storPoolMap = getStorPoolMap(
                        vlm,
                        apiCtx
                    );

                    for (StorPool storPool : storPoolMap.values())
                    {
                        DeviceProviderKind providerKind = storPool.getDeviceProviderKind();
                        boolean supportsSnapshot;
                        if (providerKind.equals(DeviceProviderKind.FILE) ||
                            providerKind.equals(DeviceProviderKind.FILE_THIN)
                        )
                        {
                            supportsSnapshot = storPool.isSnapshotSupported(apiCtx);
                        }
                        else
                        {
                            supportsSnapshot = providerKind.isSnapshotSupported();
                        }

                        if (!supportsSnapshot)
                        {
                            throw new ApiRcException(ApiCallRcImpl
                                .entryBuilder(
                                    ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                                    "Storage driver '" + providerKind + "' " + "does not support snapshots."
                                )
                                .setDetails("Used for storage pool '" + storPool.getName() + "'" +
                                    " on '" + rsc.getNode().getName() + "'.")
                                .build()
                            );
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            StateFlags<Flags> stateFlags = rsc.getStateFlags();
            isDiskless = stateFlags.isSet(apiCtx, Resource.Flags.DRBD_DISKLESS) ||
                stateFlags.isSet(apiCtx, Resource.Flags.NVME_INITIATOR);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    private SnapshotDefinition createSnapshotDfnData(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDefinition.Flags[] snapshotDfnInitFlags
    )
    {
        SnapshotDefinition snapshotDfn;
        try
        {
            snapshotDfn = snapshotDefinitionFactory.create(
                peerAccCtx.get(),
                rscDfn,
                snapshotName,
                snapshotDfnInitFlags
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotDfnDescriptionInline(rscDfn.getName().displayValue, snapshotName.displayValue),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN, String.format(
                "A snapshot definition with the name '%s' already exists in resource definition '%s'.",
                snapshotName,
                rscDfn.getName().displayValue
            )), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshotDfn;
    }

    private SnapshotVolumeDefinition createSnapshotVlmDfnData(SnapshotDefinition snapshotDfn, VolumeDefinition vlmDfn)
    {
        String descriptionInline = getSnapshotVlmDfnDescriptionInline(
            snapshotDfn.getResourceName().displayValue,
            snapshotDfn.getName().displayValue,
            vlmDfn.getVolumeNumber().value
        );
        long volumeSize = getVolumeSize(vlmDfn);

        SnapshotVolumeDefinition snapshotVlmDfn;
        try
        {
            snapshotVlmDfn = snapshotVolumeDefinitionControllerFactory.create(
                peerAccCtx.get(),
                snapshotDfn,
                vlmDfn,
                volumeSize,
                new SnapshotVolumeDefinition.Flags[]{}
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + descriptionInline,
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_EXISTS_SNAPSHOT_DFN, String.format(
                "Volume %d of snapshot definition with the name '%s' already exists in resource definition '%s'.",
                vlmDfn.getVolumeNumber().value,
                snapshotDfn.getName().displayValue,
                snapshotDfn.getResourceName().displayValue
            )), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (MdException mdExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_VLM_SIZE, String.format(
                "The " + descriptionInline + " has an invalid size of '%d'. " +
                    "Valid sizes range from %d to %d.",
                volumeSize,
                MetaData.DRBD_MIN_NET_kiB,
                MetaData.DRBD_MAX_kiB
            )), mdExc);
        }
        return snapshotVlmDfn;
    }

    private Snapshot createSnapshot(SnapshotDefinition snapshotDfn, Resource rsc)
    {
        String snapshotNameStr = snapshotDfn.getName().displayValue;
        String rscNameStr = rsc.getDefinition().getName().displayValue;
        String nodeNameStr = rsc.getNode().getName().displayValue;

        Snapshot snapshot;
        try
        {
            snapshot = snapshotFactory.create(
                peerAccCtx.get(),
                rsc,
                snapshotDfn,
                new Snapshot.Flags[0]
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotDescriptionInline(
                    Collections.singletonList(nodeNameStr),
                    rscNameStr,
                    snapshotNameStr
                ),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_EXISTS_SNAPSHOT, String.format(
                "A snapshot with the name '%s' of the resource '%s' on '%s' already exists.",
                snapshotNameStr,
                rscNameStr,
                nodeNameStr
            )), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapshot;
    }

    private SnapshotVolume createSnapshotVolume(
        Resource rsc,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
    {
        SnapshotVolume snapVlm;
        try
        {
            snapVlm = snapshotVolumeControllerFactory.create(
                peerAccCtx.get(),
                rsc,
                snapshot,
                snapshotVolumeDefinition
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register " + getSnapshotVlmDescriptionInline(
                    snapshot.getNodeName(),
                    snapshot.getResourceName(),
                    snapshot.getSnapshotName(),
                    snapshotVolumeDefinition.getVolumeNumber()
                ),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_EXISTS_SNAPSHOT, String.format(
                "Volume %d of snapshot '%s' of the resource '%s' on '%s' already exists.",
                snapshotVolumeDefinition.getVolumeNumber().value,
                snapshot.getSnapshotName(),
                snapshot.getResourceName(),
                snapshot.getNodeName()
            )), dataAlreadyExistsExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        return snapVlm;
    }

    @Deprecated
    private void setSuspend(Snapshot snapshot)
    {
        try
        {
            snapshot.setSuspendResource(peerAccCtx.get(), true);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set resource suspension for " + getSnapshotDescriptionInline(snapshot),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
    }

    private void setSuspend(Resource rsc, boolean suspend)
    {
        try
        {
            rsc.getLayerData(peerAccCtx.get()).setSuspendIo(suspend);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set resource suspension for " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private List<String> getDrbdMetaDiskPath(Volume vlm)
    {
        List<String> metaDiskPaths = new ArrayList<>();
        try
        {
            List<AbsRscLayerObject<Resource>> drbdRscDataList = LayerUtils.getChildLayerDataByKind(
                vlm.getAbsResource().getLayerData(apiCtx), DeviceLayerKind.DRBD
            );

            for (AbsRscLayerObject<Resource> rscLayerObject : drbdRscDataList)
            {
                DrbdRscData<Resource> drbdRscData = (DrbdRscData<Resource>) rscLayerObject;
                for (DrbdVlmData<Resource> drbdVlmData : drbdRscData.getVlmLayerObjects().values())
                {
                    String meta = drbdVlmData.getMetaDiskPath();
                    if (meta != null && !meta.isEmpty() && !meta.equalsIgnoreCase("internal"))
                    {
                        metaDiskPaths.add(meta);
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get meta disk path of " + getVlmDescriptionInline(vlm),
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        return metaDiskPaths;
    }

    private void markEncrypted(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        try
        {
            snapshotVlmDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotVolumeDefinition.Flags.ENCRYPTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotVlmDfnDescriptionInline(snapshotVlmDfn) + " as encrypted",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private boolean isEncrypted(VolumeDefinition vlmDfn)
    {
        boolean isEncrypted;
        try
        {
            isEncrypted = vlmDfn.getFlags().isSet(peerAccCtx.get(), VolumeDefinition.Flags.ENCRYPTED);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check whether " + getVlmDfnDescriptionInline(vlmDfn) + " is encrypted",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return isEncrypted;
    }

    private long getVolumeSize(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        try
        {
            volumeSize = vlmDfn.getVolumeSize(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "get size of " + getVlmDfnDescriptionInline(vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return volumeSize;
    }

    private Iterator<VolumeDefinition> iterateVolumeDfn(ResourceDefinition rscDfn)
    {
        Iterator<VolumeDefinition> vlmDfnIter;
        try
        {
            vlmDfnIter = rscDfn.iterateVolumeDfn(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "iterate the volume definitions of " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return vlmDfnIter;
    }

    private void setInCreation(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.setInCreation(peerAccCtx.get(), true);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "mark " + getSnapshotDfnDescriptionInline(snapshotDfn) + " in creation",
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_DFN
            );
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unsetInCreationPrivileged(SnapshotDefinition snapshotDfn)
    {
        try
        {
            snapshotDfn.setInCreation(apiCtx, false);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void enableFlagPrivileged(SnapshotDefinition snapshotDfn, SnapshotDefinition.Flags flag)
    {
        try
        {
            snapshotDfn.getFlags().enableFlags(apiCtx, flag);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    @Deprecated
    private void unsetSuspendResourcePrivileged(Snapshot snapshot)
    {
        try
        {
            snapshot.setSuspendResource(apiCtx, false);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
    }

    private void resumeIoPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource rsc= rscIt.next();
                rsc.getLayerData(apiCtx).setSuspendIo(false);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    private Collection<Snapshot> getAllSnapshotsPrivileged(SnapshotDefinition snapshotDfn)
    {
        Collection<Snapshot> allSnapshots;
        try
        {
            allSnapshots = snapshotDfn.getAllSnapshots(apiCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        return allSnapshots;
    }

    private void setTakeSnapshotPrivileged(Snapshot snapshot, boolean takeSnapshot)
    {
        try
        {
            snapshot.setTakeSnapshot(apiCtx, takeSnapshot);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
    }

    private static boolean isFailNotConnected(CtrlResponseUtils.DelayedApiRcException exception)
    {
        return exception.getErrors().stream()
            .flatMap(apiRcException -> apiRcException.getApiCallRc().getEntries().stream())
            .anyMatch(rcEntry -> rcEntry.getReturnCode() == ApiConsts.FAIL_NOT_CONNECTED);
    }

    private static CtrlSatelliteUpdateCaller.NotConnectedHandler notConnectedCannotAbort()
    {
        return nodeName -> Flux.error(new ApiRcException(ApiCallRcImpl
            .entryBuilder(
                ApiConsts.WARN_NOT_CONNECTED,
                "Unable to abort snapshot process on disconnected satellite '" + nodeName + "'"
            )
            .setDetails("IO may be suspended until the connection to the satellite is re-established")
            .build()
        ));
    }
}
