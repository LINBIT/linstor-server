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
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDataControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinitionData;
import com.linbit.linstor.core.objects.SnapshotDefinitionDataControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDataControllerFactory;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinitionControllerFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.SnapshotDefinition.SnapshotDfnFlags;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition.SnapshotVlmDfnFlags;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.CtrlLayerDataHelper;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;

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
    private final SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactory;
    private final SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactory;
    private final SnapshotDataControllerFactory snapshotDataFactory;
    private final SnapshotVolumeDataControllerFactory snapshotVolumeDataControllerFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlLayerDataHelper layerStackHelper;

    @Inject
    public CtrlSnapshotCrtApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlSnapshotHelper ctrlSnapshotHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        SnapshotDefinitionDataControllerFactory snapshotDefinitionDataFactoryRef,
        SnapshotVolumeDefinitionControllerFactory snapshotVolumeDefinitionControllerFactoryRef,
        SnapshotDataControllerFactory snapshotDataFactoryRef,
        SnapshotVolumeDataControllerFactory snapshotVolumeDataControllerFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlLayerDataHelper layerStackHelperRef
    )
    {
        apiCtx = apiCtxRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlSnapshotHelper = ctrlSnapshotHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        snapshotDefinitionDataFactory = snapshotDefinitionDataFactoryRef;
        snapshotVolumeDefinitionControllerFactory = snapshotVolumeDefinitionControllerFactoryRef;
        snapshotDataFactory = snapshotDataFactoryRef;
        snapshotVolumeDataControllerFactory = snapshotVolumeDataControllerFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        peerAccCtx = peerAccCtxRef;
        layerStackHelper = layerStackHelperRef;
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
        final ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameStr, true);
        final ResourceName rscName = rscDfn.getName();

        SnapshotName snapshotName = LinstorParsingUtils.asSnapshotName(snapshotNameStr);
        SnapshotDefinition snapshotDfn = createSnapshotDfnData(
            rscDfn,
            snapshotName,
            new SnapshotDfnFlags[] {}
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

            Map<String, String> vlmDfnProps = getVlmDfnProps(vlmDfn).map();
            Map<String, String> snapshotVlmDfnProps = getSnapshotVlmDfnProps(snapshotVlmDfn).map();

            boolean isEncrypted = isEncrypted(vlmDfn);
            if (isEncrypted)
            {

                String cryptPasswd = vlmDfnProps.get(ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD);
                if (cryptPasswd == null)
                {
                    throw new ImplementationError("Encrypted volume definition without crypt passwd found");
                }

                markEncrypted(snapshotVlmDfn);
                snapshotVlmDfnProps.put(
                    ApiConsts.KEY_STOR_POOL_CRYPT_PASSWD,
                    cryptPasswd
                );
            }

            copyProp(vlmDfnProps, snapshotVlmDfnProps, ApiConsts.KEY_STOR_POOL_OVERRIDE_VLM_ID);
            copyProp(vlmDfnProps, snapshotVlmDfnProps, ApiConsts.KEY_DRBD_CURRENT_GI);
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

        ctrlTransactionHelper.commit();

        ApiCallRcImpl responses = new ApiCallRcImpl();

        responses.addEntry(ApiSuccessUtils.defaultRegisteredEntry(
            snapshotDfn.getUuid(), getSnapshotDescriptionInline(nodeNameStrs, rscNameStr, snapshotNameStr)
        ));

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
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
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        SnapshotDfnFlags flag = exception instanceof CtrlResponseUtils.DelayedApiRcException &&
            isFailNotConnected((CtrlResponseUtils.DelayedApiRcException) exception) ?
            SnapshotDfnFlags.FAILED_DISCONNECT :
            SnapshotDfnFlags.FAILED_DEPLOYMENT;

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
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

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
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            unsetSuspendResourcePrivileged(snapshot);
        }

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> satelliteUpdateResponses =
            ctrlSatelliteUpdateCaller.updateSatellites(snapshotDfn, notConnectedError())
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
        SnapshotDefinitionData snapshotDfn = ctrlApiDataLoader.loadSnapshotDfn(rscName, snapshotName, true);

        unsetInCreationPrivileged(snapshotDfn);

        for (Snapshot snapshot : getAllSnapshotsPrivileged(snapshotDfn))
        {
            setTakeSnapshotPrivileged(snapshot, false);
        }

        enableFlagPrivileged(snapshotDfn, SnapshotDfnFlags.SUCCESSFUL);

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
        Snapshot snapshot = createSnapshotData(snapshotDfn, rsc);
        setSuspendResource(snapshot);

        for (SnapshotVolumeDefinition snapshotVolumeDefinition : snapshotVolumeDefinitions)
        {
            createSnapshotVolumeData(rsc, snapshot, snapshotVolumeDefinition);
        }
    }

    private void ensureSnapshotsViable(ResourceDefinitionData rscDfn)
    {
        Iterator<Resource> rscIterator = ctrlSnapshotHelper.iterateResource(rscDfn);
        while (rscIterator.hasNext())
        {
            Resource currentRsc = rscIterator.next();
            ensureDriversSupportSnapshots(currentRsc);
            ensureInternalMetaDisks(currentRsc);
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
                        apiCtx,
                        CtrlVlmListApiCallHandler::getVlmDescriptionInline
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
                                    " on '" + rsc.getAssignedNode().getName() + "'.")
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

    private void ensureInternalMetaDisks(Resource rsc)
    {
        Iterator<Volume> vlmIterator = rsc.iterateVolumes();
        while (vlmIterator.hasNext())
        {
            Volume vlm = vlmIterator.next();

            List<String> metaDiskPaths = getDrbdMetaDiskPath(vlm);
            boolean hasInvalidMetaDisk = false;
            for (String metaDiskPath : metaDiskPaths)
            {
                if (!metaDiskPath.equals("internal"))
                {
                    hasInvalidMetaDisk = true;
                    break;
                }
            }
            if (hasInvalidMetaDisk)
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_SNAPSHOTS_NOT_SUPPORTED,
                        "Snapshot with external meta-disk not supported."
                    )
                    .setDetails("Volume " + vlm.getVolumeDefinition().getVolumeNumber().value +
                        " on node " + rsc.getAssignedNode().getName().displayValue +
                        " has meta disk path '" + metaDiskPaths + "'")
                    .build()
                );
            }
        }
    }

    private boolean isDisklessPrivileged(Resource rsc)
    {
        boolean isDiskless;
        try
        {
            isDiskless = rsc.getStateFlags().isSet(apiCtx, Resource.RscFlags.DISKLESS);
        }
        catch (AccessDeniedException implError)
        {
            throw new ImplementationError(implError);
        }
        return isDiskless;
    }

    private SnapshotDefinitionData createSnapshotDfnData(
        ResourceDefinition rscDfn,
        SnapshotName snapshotName,
        SnapshotDfnFlags[] snapshotDfnInitFlags
    )
    {
        SnapshotDefinitionData snapshotDfn;
        try
        {
            snapshotDfn = snapshotDefinitionDataFactory.create(
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
                vlmDfn.getVolumeNumber(),
                volumeSize,
                new SnapshotVlmDfnFlags[]{}
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

    private void copyProp(
        Map<String, String> vlmDfnProps,
        Map<String, String> snapshotVlmDfnProps,
        String propKey
    )
    {
        String propValue = vlmDfnProps.get(propKey);
        if (propValue != null)
        {
            snapshotVlmDfnProps.put(propKey, propValue);
        }
    }

    private Snapshot createSnapshotData(SnapshotDefinition snapshotDfn, Resource rsc)
    {
        String snapshotNameStr = snapshotDfn.getName().displayValue;
        String rscNameStr = rsc.getDefinition().getName().displayValue;
        String nodeNameStr = rsc.getAssignedNode().getName().displayValue;

        Snapshot snapshot;
        try
        {
            NodeId nodeId = null;
            List<RscLayerObject> childLayerDataByKind = LayerUtils.getChildLayerDataByKind(
                rsc.getLayerData(apiCtx),
                DeviceLayerKind.DRBD
            );
            if (!childLayerDataByKind.isEmpty())
            {
                nodeId = ((DrbdRscData) childLayerDataByKind.get(0)).getNodeId();
            }

            snapshot = snapshotDataFactory.create(
                peerAccCtx.get(),
                rsc.getAssignedNode(),
                snapshotDfn,
                nodeId,
                new Snapshot.SnapshotFlags[]{},
                layerStackHelper.getLayerStack(rsc)
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

    private void createSnapshotVolumeData(
        Resource rsc,
        Snapshot snapshot,
        SnapshotVolumeDefinition snapshotVolumeDefinition
    )
    {
        try
        {
            snapshotVolumeDataControllerFactory.create(
                peerAccCtx.get(),
                snapshot,
                snapshotVolumeDefinition,
                getStorPoolMap(
                    rsc.getVolume(snapshotVolumeDefinition.getVolumeNumber()),
                    apiCtx,
                    CtrlVlmListApiCallHandler::getVlmDescriptionInline
                ).get("")
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
    }

    private void setSuspendResource(Snapshot snapshot)
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

    private Props getVlmDfnProps(VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getVlmDfnDescriptionInline(vlmDfn),
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    private Props getSnapshotVlmDfnProps(SnapshotVolumeDefinition snapshotVlmDfn)
    {
        Props props;
        try
        {
            props = snapshotVlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getSnapshotVlmDfnDescriptionInline(snapshotVlmDfn),
                ApiConsts.FAIL_ACC_DENIED_SNAPSHOT_VLM_DFN
            );
        }
        return props;
    }

    private List<String> getDrbdMetaDiskPath(Volume vlm)
    {
        List<String> metaDiskPaths = new ArrayList<>();
        try
        {
            List<RscLayerObject> drbdRscDataList = LayerUtils.getChildLayerDataByKind(
                vlm.getResource().getLayerData(apiCtx), DeviceLayerKind.DRBD
            );

            for (RscLayerObject rscLayerObject : drbdRscDataList)
            {
                DrbdRscData drbdRscData = (DrbdRscData) rscLayerObject;
                for (DrbdVlmData drbdVlmData : drbdRscData.getVlmLayerObjects().values())
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
            snapshotVlmDfn.getFlags().enableFlags(peerAccCtx.get(), SnapshotVlmDfnFlags.ENCRYPTED);
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
            isEncrypted = vlmDfn.getFlags().isSet(peerAccCtx.get(), VolumeDefinition.VlmDfnFlags.ENCRYPTED);
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

    private Iterator<VolumeDefinition> iterateVolumeDfn(ResourceDefinitionData rscDfn)
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

    private void enableFlagPrivileged(SnapshotDefinitionData snapshotDfn, SnapshotDfnFlags flag)
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

    private Collection<Snapshot> getAllSnapshotsPrivileged(SnapshotDefinitionData snapshotDfn)
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
