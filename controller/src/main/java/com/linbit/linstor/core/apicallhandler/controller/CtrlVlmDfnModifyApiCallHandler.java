package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.exceptions.InvalidSizeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.SatelliteResourceStateDrbdUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.VolumeDefinitionResizeCheckUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.ebs.EbsStatusManagerService;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition.Flags;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerSizeHelper;
import com.linbit.linstor.layer.storage.ebs.EbsUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.Base64;
import com.linbit.utils.PairNonNull;
import com.linbit.utils.TimeUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.getVlmDfnDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlVlmDfnApiCallHandler.makeVlmDfnContext;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlVlmDfnModifyApiCallHandler implements CtrlSatelliteConnectionListener
{
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final long EBS_DFLT_COOLDOWN_PERIOD_IN_SEC = TimeUnit.HOURS.toSeconds(6) +
        TimeUnit.MINUTES.toSeconds(5); // 6 hours and 5 min in sec

    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final LockGuardFactory lockGuardFactory;
    private final BackupInfoManager backupInfoMgr;
    private final EbsStatusManagerService ebsStatusMgr;
    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilder;
    private final EncryptionHelper encHelper;
    private final LayerSizeHelper layerSizeHelper;

    @Inject
    CtrlVlmDfnModifyApiCallHandler(
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        LockGuardFactory lockGuardFactoryRef,
        BackupInfoManager backupInfoMgrRef,
        EbsStatusManagerService ebsStatusMgrRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderRef,
        EncryptionHelper encryptionHelperRef,
        ErrorReporter errorReporterRef,
        LayerSizeHelper layerSizeHelperRef
    )
    {
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        lockGuardFactory = lockGuardFactoryRef;
        backupInfoMgr = backupInfoMgrRef;
        ebsStatusMgr = ebsStatusMgrRef;
        propsChangeListenerBuilder = propsChangeListenerBuilderRef;
        encHelper = encryptionHelperRef;
        errorReporter = errorReporterRef;
        layerSizeHelper = layerSizeHelperRef;
    }

    @Override
    public Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn, ResponseContext context)
    {
        List<Flux<ApiCallRc>> fluxes = new ArrayList<>();

        ResourceName rscName = rscDfn.getName();

        Iterator<VolumeDefinition> vlmDfnIter = rscDfn.iterateVolumeDfn();
        while (vlmDfnIter.hasNext())
        {
            VolumeDefinition vlmDfn = vlmDfnIter.next();
            boolean resizing = vlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE);
            if (resizing)
            {
                fluxes.add(updateSatellites(rscName, vlmDfn.getVolumeNumber()));
            }
        }

        return fluxes;
    }

    public Flux<ApiCallRc> modifyVlmDfn(
        @Nullable UUID vlmDfnUuid,
        String rscName,
        int vlmNr,
        Long size,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        List<String> vlmDfnFlags
    )
    {
        ResponseContext context = makeVlmDfnContext(
            ApiOperation.makeModifyOperation(),
            rscName,
            vlmNr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify volume definition",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> modifyVlmDfnInTransaction(
                    vlmDfnUuid,
                    rscName,
                    vlmNr,
                    size,
                    overrideProps,
                    deletePropKeys,
                    vlmDfnFlags
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyVlmDfnInTransaction(
        @Nullable UUID vlmDfnUuid,
        String rscNameStr,
        int vlmNrInt,
        @Nullable Long size,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        List<String> vlmDfnFlagsRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        boolean notifyStlts = false;
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        VolumeNumber vlmNr = LinstorParsingUtils.asVlmNr(vlmNrInt);
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr);

        if (vlmDfnUuid != null && !vlmDfnUuid.equals(vlmDfn.getUuid()))
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_UUID_VLM_DFN,
                "UUID check failed. Given UUID: " + vlmDfnUuid + ". Persisted UUID: " + vlmDfn.getUuid()
            ));
        }
        if (backupInfoMgr.restoreContainsRscDfn(vlmDfn.getResourceDefinition()))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_IN_USE,
                    rscNameStr + " is currently being restored from a backup. " +
                        "Please wait until the restore is finished"
                )
            );
        }

        Props vlmDfnProps = getVlmDfnProps(vlmDfn);

        List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
        prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

        boolean changedEbsPropsWithAction = propChangeCausingEbsAction(vlmDfnProps, overrideProps);

        List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
        Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilder.get()
            .buildPropsChangedListeners(vlmDfn, specialPropFluxes);

        notifyStlts = ctrlPropsHelper.fillProperties(
            responses,
            LinStorObject.VLM_DFN,
            overrideProps,
            vlmDfnProps,
            ApiConsts.FAIL_ACC_DENIED_VLM_DFN,
            prefixesIgnoringWhitelistCheck,
            propsChangedListeners
        ) || notifyStlts;

        try
        {
            notifyStlts = ctrlPropsHelper.remove(
                responses,
                LinStorObject.VLM_DFN,
                vlmDfnProps,
                deletePropKeys,
                Collections.emptyList(),
                prefixesIgnoringWhitelistCheck,
                propsChangedListeners
            ) || notifyStlts;
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }

        PairNonNull<Set<Flags>, Set<Flags>> flagPair = FlagsHelper.extractFlagsToEnableOrDisable(
            VolumeDefinition.Flags.class,
            vlmDfnFlagsRef
        );

        boolean isGrossFlagCurrentlySet = isFlagSet(vlmDfn, VolumeDefinition.Flags.GROSS_SIZE);
        boolean shouldGrossFlagBeEnabled = !isGrossFlagCurrentlySet &&
            flagPair.objA.contains(VolumeDefinition.Flags.GROSS_SIZE);
        boolean shouldGrossFlagBeDisabled = isGrossFlagCurrentlySet &&
            flagPair.objB.contains(VolumeDefinition.Flags.GROSS_SIZE);

        boolean updateForResize = false;

        if (shouldGrossFlagBeDisabled)
        {
            unsetFlag(vlmDfn, VolumeDefinition.Flags.GROSS_SIZE);
            updateForResize = true;
        }
        else
        if (shouldGrossFlagBeEnabled)
        {
            if (hasDeployedVolumes(vlmDfn))
            {
                VolumeDefinitionResizeCheckUtils.ensureShrinkingIsSupported(vlmDfn);
            }
            setFlag(vlmDfn, VolumeDefinition.Flags.GROSS_SIZE);
            updateForResize = true;
        }

        if (size != null)
        {
            long diffSize = size - getVlmDfnSize(vlmDfn);

            boolean shrink = diffSize < 0;
            if (shrink)
            {
                VolumeDefinitionResizeCheckUtils.ensureShrinkingIsSupported(vlmDfn);
                setFlag(vlmDfn, VolumeDefinition.Flags.RESIZE_SHRINK);
                updateForResize = true;
                notifyStlts = true;
                setVlmDfnSize(vlmDfn, size);
            }
            else if (diffSize == 0)
            {
                responses.add(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_VLMDFN_RESIZE_SAME_SIZE,
                        "VolumeDefinition already has the expected size, no satellite update will be triggered."));
            }
            else
            {
                VolumeDefinitionResizeCheckUtils.ensureAllStorPoolsHaveEnoughFreeSpace(vlmDfn, diffSize);

                VolumeDefinitionResizeCheckUtils.ensureExactSizeIsUnset(vlmDfn);

                updateForResize = true;
                notifyStlts = true;
                setVlmDfnSize(vlmDfn, size);
            }
        }

        if (hasEbsResource(vlmDfn))
        {
            ensureAllowedEbsAction(vlmDfn, size != null, changedEbsPropsWithAction);
        }

        Flux<ApiCallRc> updateResponses = Flux.empty();
        if (updateForResize)
        {
            VolumeDefinitionResizeCheckUtils.ensureNoThickLvmSnapshots(vlmDfn);
            VolumeDefinitionResizeCheckUtils.ensureSharedDataNotActiveOnMultipleNodes(vlmDfn);
            ensureAllDrbdVolumesUpToDate(vlmDfn);

            /*
             * If the VlmDfn will grow in size, we have to
             * * set the RESIZE flag on all volumes
             * * let all satellites do an update
             * * set the DRBD_RESIZE flag on one volume (DRBD-resizing is cluster-aware)
             * * let all satellites do the update where actually only one performs the DRBD resize.
             * * unset all RESIZE and DRBD_RESIZE flags
             *
             * If the VlmDfn will shrink, we have to:
             * * set the DRBD_RESIZE flag on one volume + update stlts
             * * set the RESIZE flag on all volumes + updatestlts
             * * unset RESIZE and DRBD_RESIZE flags
             *
             * Note: Satellites do not care about vlmDfn RESIZE flag, only about vlm RESIZE and DRBD_RESIZE flag
             */
            Iterator<Volume> vlmIter = iterateVolumes(vlmDfn);

            if (vlmIter.hasNext())
            {
                markVlmDfnResize(vlmDfn);
            }

            notifyStlts = true;
        }

        ctrlTransactionHelper.commit();

        errorReporter.logInfo("Volume definition modified %s/%d/%s", rscNameStr, vlmNr.getValue(), notifyStlts);

        if (notifyStlts)
        {
            updateResponses = updateResponses.concatWith(updateSatellites(rscName, vlmNr));
        }


        responses.addEntry(ApiSuccessUtils.defaultModifiedEntry(vlmDfn.getUuid(), getVlmDfnDescriptionInline(vlmDfn)));

        return Flux.just((ApiCallRc) responses)
            .concatWith(updateResponses)
            .concatWith(Flux.merge(specialPropFluxes));
    }

    /**
     * Internal entry point for growing a volume definition, e.g. from the clone flow.
     * In contrast to {@link #modifyVlmDfn}, errors are propagated as error signals instead of being
     * converted into response entries, so a calling flux chain can react to failures.
     */
    public Flux<ApiCallRc> resizeVlmDfn(
        ResourceName rscName,
        VolumeNumber vlmNr,
        long newSizeKib,
        boolean checkDrbdUpToDate
    )
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resize volume definition",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resizeVlmDfnInTransaction(rscName, vlmNr, newSizeKib, checkDrbdUpToDate)
            );
    }

    private Flux<ApiCallRc> resizeVlmDfnInTransaction(
        ResourceName rscName,
        VolumeNumber vlmNr,
        long newSizeKib,
        boolean checkDrbdUpToDate
    )
    {
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr);

        long diffSize = newSizeKib - getVlmDfnSize(vlmDfn);
        if (diffSize < 0)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    "Cannot resize " + getVlmDfnDescriptionInline(vlmDfn) + " to " + newSizeKib +
                        "KiB, shrinking is not supported here."
                )
            );
        }

        Flux<ApiCallRc> flux = Flux.empty();
        if (diffSize > 0)
        {
            VolumeDefinitionResizeCheckUtils.ensureAllStorPoolsHaveEnoughFreeSpace(vlmDfn, diffSize);
            VolumeDefinitionResizeCheckUtils.ensureExactSizeIsUnset(vlmDfn);
            VolumeDefinitionResizeCheckUtils.ensureNoThickLvmSnapshots(vlmDfn);
            VolumeDefinitionResizeCheckUtils.ensureSharedDataNotActiveOnMultipleNodes(vlmDfn);
            if (checkDrbdUpToDate)
            {
                ensureAllDrbdVolumesUpToDate(vlmDfn);
            }

            setVlmDfnSize(vlmDfn, newSizeKib);
            if (iterateVolumes(vlmDfn).hasNext())
            {
                markVlmDfnResize(vlmDfn);
            }

            ctrlTransactionHelper.commit();

            errorReporter.logInfo(
                "Volume definition resized %s/%d to %dKiB",
                rscName.displayValue,
                vlmNr.getValue(),
                newSizeKib
            );

            flux = updateSatellites(rscName, vlmNr);
        }
        return flux;
    }

    private void ensureAllDrbdVolumesUpToDate(VolumeDefinition vlmDfn)
    {
        Iterator<Resource> itRsc = vlmDfn.getResourceDefinition().iterateResource();
        while (itRsc.hasNext())
        {
            final Resource rsc = itRsc.next();
            if (!rsc.isDiskless() &&
                rsc.hasDrbd() &&
                !SatelliteResourceStateDrbdUtils.allVolumesUpToDate(rsc, false))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_NOT_ALL_UPTODATE,
                        "Cannot resize volume, because we have a non-UpToDate DRBD device."
                    )
                        .setSkipErrorReport(true)
                        .build()
                );
            }
        }
    }

    public Flux<ApiCallRc> modifyVlmDfnPassphrase(
        String rscName,
        int vlmNr,
        String passphrase
    )
    {
        ResponseContext context = makeVlmDfnContext(
            ApiOperation.makeModifyOperation(),
            rscName,
            vlmNr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify volume definition passphrase",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> modifyVlmDfnPassphraseInTransaction(
                    rscName,
                    vlmNr,
                    passphrase
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyVlmDfnPassphraseInTransaction(
        String rscNameStr,
        int vlmNrInt,
        String passphrase
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
        VolumeNumber vlmNr = LinstorParsingUtils.asVlmNr(vlmNrInt);
        VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfn(rscName, vlmNr);

        Props vlmDfnProps = getVlmDfnProps(vlmDfn);

        try
        {
            byte[] encPassphrase = encHelper.encrypt(passphrase);
            vlmDfnProps.setProp(ApiConsts.NAMESPC_ENCRYPTION + "/" + ApiConsts.KEY_PASSPHRASE,
                    Base64.encode(encPassphrase));

            final List<Resource> rscs = vlmDfn.getResourceDefinition().streamResource()
                .collect(Collectors.toList());

            boolean passModified = false;
            for (var rsc : rscs)
            {
                var luksRscLayerSet = LayerRscUtils.getRscDataByLayer(rsc.getLayerData(), DeviceLayerKind.LUKS);
                if (!luksRscLayerSet.isEmpty())
                {
                    passModified = true;
                    break;
                }
            }
            if (!passModified)
            {
                responses.addEntry(
                    "No resources have any luks layer, no passphrase has been changed.",
                    ApiConsts.WARN_NOT_FOUND
                );
            }
        }
        catch (LinStorException | InvalidValueException exc)
        {
            throw new ApiException(exc);
        }

        ctrlTransactionHelper.commit();

        Flux<ApiCallRc> updateResponses = updateSatellites(rscName, vlmNr);

        responses.addEntry(ApiSuccessUtils.defaultModifiedEntry(vlmDfn.getUuid(), getVlmDfnDescriptionInline(vlmDfn)));

        return Flux.just((ApiCallRc) responses)
            .concatWith(updateResponses);
    }

    private boolean propChangeCausingEbsAction(
        ReadOnlyProps vlmDfnPropsRef,
        Map<String, String> overridePropsRef
    )
    {
        boolean ret = false;
        String ebsVlmTypeVal = overridePropsRef.get(ApiConsts.KEY_EBS_VOLUME_TYPE);
        if (ebsVlmTypeVal != null && !ebsVlmTypeVal.equals(vlmDfnPropsRef.getProp(ApiConsts.KEY_EBS_VOLUME_TYPE)))
        {
            ret = true;
        }
        // deleting prop does not cause change. EBS volume will stay as it is

        return ret;
    }

    private boolean hasEbsResource(VolumeDefinition vlmDfnRef)
    {
        boolean hasEbsResource = false;
        Iterator<Resource> rscIt;
        rscIt = vlmDfnRef.getResourceDefinition().iterateResource();
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (EbsUtils.isEbs(rsc))
            {
                hasEbsResource = true;
                break;
            }
        }
        return hasEbsResource;
    }

    private void ensureAllowedEbsAction(
        VolumeDefinition vlmDfnRef,
        boolean sizeChangesRef,
        boolean propChangeCausingEbsAction
    )
    {
        if (sizeChangesRef || propChangeCausingEbsAction)
        {
            try
            {
                Props props = vlmDfnRef.getProps();
                String lastModStr = props.getProp(
                    InternalApiConsts.KEY_EBS_COOLDOWN_UNTIL_TIMESTAMP,
                    ApiConsts.NAMESPC_EBS
                );
                long nowInSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

                long cooldownUntil;
                if (lastModStr != null && !lastModStr.isEmpty())
                {
                    cooldownUntil = Long.parseLong(lastModStr);
                    long coolingDownRemainingSecs = cooldownUntil - nowInSeconds;
                    if (coolingDownRemainingSecs > 0)
                    {
                        long hours = TimeUnit.SECONDS.toHours(coolingDownRemainingSecs);
                        long hoursInSec = TimeUnit.HOURS.toSeconds(hours);

                        long min = TimeUnit.SECONDS.toMinutes(coolingDownRemainingSecs - hoursInSec);
                        long secs = coolingDownRemainingSecs - hoursInSec - TimeUnit.MINUTES.toSeconds(min);
                        throw new ApiRcException(
                            ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_EBS_COOLDOWN,
                                String.format(
                                    "EBS volumes need a cooldown period of 6 hours. You still need to wait " +
                                    "until %s (%02d:%02d:%02d left)",
                                    props.getProp(InternalApiConsts.KEY_EBS_COOLDOWN_UNTIL, ApiConsts.NAMESPC_EBS),
                                    hours,
                                    min,
                                    secs
                                )
                            )
                        );
                    }
                }

                props.setProp(
                    InternalApiConsts.KEY_EBS_COOLDOWN_UNTIL_TIMESTAMP,
                    Long.toString(nowInSeconds + EBS_DFLT_COOLDOWN_PERIOD_IN_SEC),
                    ApiConsts.NAMESPC_EBS
                );
                props.setProp(
                    InternalApiConsts.KEY_EBS_COOLDOWN_UNTIL,
                    DATE_TIME_FORMATTER.format(
                        TimeUtils.toLocalZonedDateTime((nowInSeconds + EBS_DFLT_COOLDOWN_PERIOD_IN_SEC) * 1000)
                    ),
                    ApiConsts.NAMESPC_EBS
                );
            }
            catch (InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
    }

    // Restart from here when connection established and RESIZE flag set
    private Flux<ApiCallRc> updateSatellites(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Update for volume definition modification",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> updateSatellitesInScope(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> updateSatellitesInScope(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Flux<ApiCallRc> nextStep = Flux.empty();
            boolean resize;
            boolean shrink;
            shrink = vlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE_SHRINK);
            resize = vlmDfn.getFlags().isSet(VolumeDefinition.Flags.RESIZE);
            if (resize)
            {
                /*
                 * TODO: we should not call the first flux as *InTransaction method, but just call the regular method
                 * instead.
                 * However, if we change that to the non-*InTransaction version, the RESIZE flag is not set on vlms,
                 * just the vlmDfn.size is updated. That - for some reasons - is enough for the satellites to execute
                 * lvresize.
                 *
                 * This was debugged with an intentionally broken /sbin/lvresize. The result was that the resize
                 * operation stopped (expected), but in a state where the linstor-client would not show any of the
                 * resources in resizing state, just one of them having the updated size (the one with the still working
                 * lvresize) and the one with the broken lvresize still had the old allocated size. No
                 * "Resizing, UpToDate". Just "UpToDate" with different allocated sizes.
                 */
                if (shrink)
                {
                    // if drbd exists, resize DRBD first, others afterwards

                    boolean firstFlux = true;
                    if (hasDrbd(vlmDfn))
                    {
                        nextStep = resizeDrbdInTransaction(rscName, vlmNr);
                        firstFlux = false;
                    }
                    if (firstFlux)
                    {
                        nextStep = resizeNonDrbdInTransaction(rscName, vlmNr);
                    }
                    else
                    {
                        // DO NOT call the *InTx version
                        nextStep = nextStep.concatWith(resizeNonDrbd(rscName, vlmNr));
                    }
                }
                else
                {
                    // resize others first and DRBD last
                    nextStep = resizeNonDrbdInTransaction(rscName, vlmNr);
                    if (hasDrbd(vlmDfn))
                    {
                        nextStep = nextStep.concatWith(resizeDrbd(rscName, vlmNr));
                    }
                }
                // finally, finish resize
                nextStep = nextStep.concatWith(finishResize(rscName, vlmNr));
            }
            flux = ctrlSatelliteUpdateCaller.updateSatellites(vlmDfn.getResourceDefinition(), nextStep)
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rscName,
                        "Updated volume " + vlmNr + " of {1} on {0}"
                    )
                )
                .concatWith(nextStep)
                .onErrorResume(
                CtrlResponseUtils.DelayedApiRcException.class,
                ignored -> Flux.empty()
            );
        }

        return flux;
    }

    private boolean hasDrbd(VolumeDefinition vlmDfnRef)
    {
        boolean anyResourceHasDrbdLayer;
        anyResourceHasDrbdLayer = vlmDfnRef.getResourceDefinition().streamResource()
            .anyMatch(rsc -> rsc.hasDrbd());
        return anyResourceHasDrbdLayer;
    }

    private Flux<ApiCallRc> resizeDrbd(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resize DRBD",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resizeDrbdInTransaction(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> resizeDrbdInTransaction(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Optional<Volume> drbdResizeVlm = streamVolumesPrivileged(vlmDfn)
                .filter(this::isDrbdDiskful)
                .findAny();
            drbdResizeVlm.ifPresent(this::markVlmDrbdResize);

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(vlmDfn.getResourceDefinition(), Flux.empty())
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rscName,
                        getNodeNames(drbdResizeVlm),
                        "Resized DRBD resource {1} on {0}",
                        null
                    )
                );
        }

        return flux;
    }

    private Flux<ApiCallRc> resizeNonDrbd(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resize Non DRBD",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resizeNonDrbdInTransaction(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> resizeNonDrbdInTransaction(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        Flux<ApiCallRc> flux;

        if (vlmDfn == null)
        {
            flux = Flux.empty();
        }
        else
        {
            Set<NodeName> nodeNames = new HashSet<>();
            Iterator<Volume> vlmIter = iterateVolumes(vlmDfn);
            while (vlmIter.hasNext())
            {
                Volume vlm = vlmIter.next();

                markVlmResize(vlm);
                nodeNames.add(vlm.getAbsResource().getNode().getName());
            }

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(vlmDfn.getResourceDefinition(), Flux.empty())
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rscName,
                        nodeNames,
                        "Resized resource {1} on {0}",
                        null
                    )
                );
        }

        return flux;
    }

    private boolean isDrbdDiskful(Volume vlm)
    {
        boolean diskless;
        diskless = vlm.getAbsResource().isDrbdDiskless();
        return !diskless;
    }

    private Flux<ApiCallRc> finishResize(ResourceName rscName, VolumeNumber vlmNr)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Clean up after resize",
                lockGuardFactory.create()
                    .write(LockGuardFactory.LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> finishResizeInTransaction(rscName, vlmNr)
            );
    }

    private Flux<ApiCallRc> finishResizeInTransaction(ResourceName rscName, VolumeNumber vlmNr)
    {
        @Nullable VolumeDefinition vlmDfn = ctrlApiDataLoader.loadVlmDfnOrNull(rscName, vlmNr);

        if (vlmDfn != null)
        {
            boolean ebsResize = false;
            Iterator<Volume> vlmsIt = iterateVolumes(vlmDfn);
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();

                unmarkVlmDrbdResizePrivileged(vlm);
                unmarkVlmResizePrivileged(vlm);
                if (!ebsResize)
                {
                    ebsResize |= isEbsPrivileged(vlm);
                }
            }
            unmarkVlmDfnResizePrivileged(vlmDfn);

            ctrlTransactionHelper.commit();

            if (ebsResize)
            {
                ebsStatusMgr.pollAsync();
            }
        }

        return updateSatellites(rscName, vlmNr);
    }

    private Props getVlmDfnProps(VolumeDefinition vlmDfn)
    {
        Props props;
        props = vlmDfn.getProps();
        return props;
    }

    private void markVlmDfnResize(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.getFlags().enableFlags(VolumeDefinition.Flags.RESIZE);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unmarkVlmDfnResizePrivileged(VolumeDefinition vlmDfn)
    {
        try
        {
            vlmDfn.getFlags().disableFlags(VolumeDefinition.Flags.RESIZE);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markVlmResize(Volume vlm)
    {
        try
        {
            vlm.getFlags().enableFlags(Volume.Flags.RESIZE);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unmarkVlmResizePrivileged(Volume vlm)
    {
        try
        {
            vlm.getFlags().disableFlags(Volume.Flags.RESIZE);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void markVlmDrbdResize(Volume vlm)
    {
        try
        {
            vlm.getFlags().enableFlags(Volume.Flags.DRBD_RESIZE);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private void unmarkVlmDrbdResizePrivileged(Volume vlm)
    {
        try
        {
            vlm.getFlags().disableFlags(Volume.Flags.DRBD_RESIZE);
        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
    }

    private long getVlmDfnSize(VolumeDefinition vlmDfn)
    {
        long volumeSize;
        volumeSize = vlmDfn.getVolumeSize();
        return volumeSize;
    }

    private void setVlmDfnSize(VolumeDefinition vlmDfn, Long size)
    {
        try
        {
            vlmDfn.setVolumeSize(size);

            // run size check to verify if we do not exceed some limit...
            Iterator<Volume> vlmsIt = vlmDfn.iterateVolumes();
            VolumeNumber vlmNr = vlmDfn.getVolumeNumber();
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();
                layerSizeHelper.calculateSize(
                    vlm.getAbsResource().getLayerData().getVlmProviderObject(vlmNr)
                );
            }

        }
        catch (DatabaseException sqlExc)
        {
            throw new ApiDatabaseException(sqlExc);
        }
        catch (InvalidSizeException | MinSizeException | MaxSizeException exc)
        {
            final String descr;
            if (exc instanceof MinSizeException)
            {
                descr = "too small";
            }
            else if (exc instanceof MaxSizeException)
            {
                descr = "too big";
            }
            else
            {
                descr = "invalid";
            }
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_SIZE,
                    "The given size [" + size + "KiB] is " + descr,
                    true
                )
            );
        }
    }

    private boolean hasDeployedVolumes(VolumeDefinition vlmDfn)
    {
        boolean hasVolumes;
        hasVolumes = vlmDfn.iterateVolumes().hasNext();
        return hasVolumes;
    }

    private Iterator<Volume> iterateVolumes(VolumeDefinition vlmDfn)
    {
        Iterator<Volume> volumeIterator;
        volumeIterator = vlmDfn.iterateVolumes();
        return volumeIterator;
    }

    private Stream<Volume> streamVolumesPrivileged(VolumeDefinition vlmDfn)
    {
        Stream<Volume> volumeStream;
        volumeStream = vlmDfn.streamVolumes();
        return volumeStream;
    }

    private boolean isFlagSet(VolumeDefinition vlmDfnRef, Flags flag)
    {
        boolean isFlagSet;
        isFlagSet = vlmDfnRef.getFlags().isSet(flag);
        return isFlagSet;
    }

    private void unsetFlag(VolumeDefinition vlmDfnRef, Flags flag)
    {
        try
        {
            vlmDfnRef.getFlags().disableFlags(flag);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    private void setFlag(VolumeDefinition vlmDfnRef, Flags flag)
    {
        try
        {
            vlmDfnRef.getFlags().enableFlags(flag);
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    private boolean isEbsPrivileged(Volume vlmRef)
    {
        return EbsUtils.isEbs(vlmRef.getAbsResource());
    }

    private static Set<NodeName> getNodeNames(Optional<Volume> drbdResizeVlm)
    {
        return drbdResizeVlm.isPresent() ?
            Collections.singleton(drbdResizeVlm.get().getAbsResource().getNode().getName()) :
            Collections.emptySet();
    }
}
