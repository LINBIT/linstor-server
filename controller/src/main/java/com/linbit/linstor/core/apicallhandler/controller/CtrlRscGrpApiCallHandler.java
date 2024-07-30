package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.VolumeNumberAlloc;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiCallRcWith;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.MaxVlmSizeCandidatePojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QueryAllSizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoRequestPojo;
import com.linbit.linstor.api.pojo.QuerySizeInfoResponsePojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmDfnWithCreationPayloadPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDefinitionUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceGroupApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupControllerFactory;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.StorPool.Key;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.core.repository.StorPoolDefinitionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;
import com.linbit.utils.Base64;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_GRP_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscGrpApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final AccessContext apiCtx;
    private final ResourceGroupRepository resourceGroupRepository;
    private final ResourceGroupControllerFactory rscGrpFactory;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final CtrlVlmGrpApiCallHandler ctrlVlmGrpApiCallHandler;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final CtrlQueryMaxVlmSizeHelper qmvsHelper;
    private final CtrlQuerySizeInfoHelper qsiHelper;

    private final CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandler;
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;
    private final AutoDiskfulTask autoDiskfulTask;
    private final StorPoolDefinitionRepository spdRepo;
    private final AutoSnapshotTask autoSnapshotTask;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandler;
    private final SystemConfRepository systemConfRepository;
    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilder;
    private final CtrlRscAutoTieBreakerHelper ctrlRscAutoTiebreakerHelper;
    private final CtrlRscAutoQuorumHelper ctrlRscAutoQuorumHelper;
    private final EncryptionHelper encryptionHelper;

    @Inject
    public CtrlRscGrpApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ResourceGroupRepository rscGrpRepoRef,
        ResourceGroupControllerFactory rscGrpFactoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        CtrlVlmGrpApiCallHandler ctrlVlmGrpApiCallHandlerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandlerRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef,
        CtrlQueryMaxVlmSizeHelper qmvsHelperRef,
        CtrlQuerySizeInfoHelper qsiHelperRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        LockGuardFactory lockGuardFactoryRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        StorPoolDefinitionRepository spdRepoRef,
        AutoSnapshotTask autoSnapshotTaskRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandlerRef,
        SystemConfRepository systemConfRepositoryRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderRef,
        CtrlRscAutoTieBreakerHelper ctrlRscAutoTiebreakerHelperRef,
        CtrlRscAutoQuorumHelper ctrlRscAutoQuorumHelperRef,
        EncryptionHelper encryptionHelperRef
    )
    {
        errorReporter = errorReporterRef;
        scopeRunner = scopeRunnerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        apiCtx = apiCtxRef;
        resourceGroupRepository = rscGrpRepoRef;
        rscGrpFactory = rscGrpFactoryRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        ctrlVlmGrpApiCallHandler = ctrlVlmGrpApiCallHandlerRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
        qmvsHelper = qmvsHelperRef;
        qsiHelper = qsiHelperRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        lockGuardFactory = lockGuardFactoryRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        spdRepo = spdRepoRef;
        autoSnapshotTask = autoSnapshotTaskRef;
        ctrlSnapDeleteHandler = ctrlSnapDeleteHandlerRef;
        systemConfRepository = systemConfRepositoryRef;
        propsChangeListenerBuilder = propsChangeListenerBuilderRef;
        ctrlRscAutoTiebreakerHelper = ctrlRscAutoTiebreakerHelperRef;
        ctrlRscAutoQuorumHelper = ctrlRscAutoQuorumHelperRef;
        encryptionHelper = encryptionHelperRef;
    }

    List<ResourceGroupApi> listResourceGroups(List<String> rscGrpNames, List<String> propFilters)
    {
        List<ResourceGroupApi> ret = new ArrayList<>();
        final Set<ResourceGroupName> rscGrpsFilter =
            rscGrpNames.stream().map(LinstorParsingUtils::asRscGrpName).collect(Collectors.toSet());

        try
        {
            resourceGroupRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(rscGrp ->
                    (
                        rscGrpsFilter.isEmpty() ||
                        rscGrpsFilter.contains(rscGrp.getName())
                    )
                )
                .forEach(rscGrp ->
                    {
                        try
                        {
                            final ReadOnlyProps props = rscGrp.getProps(peerAccCtx.get());
                            if (props.contains(propFilters))
                            {
                                ret.add(rscGrp.getApiData(peerAccCtx.get()));
                            }
                        }
                        catch (AccessDeniedException accDeniedExc)
                        {
                            // don't add resource group without access
                        }
                    }
                );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // return an empty list
        }

        return ret;
    }

    public ApiCallRc create(RscGrpPojo rscGrpPojoRef)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        String rscGrpNameStr = rscGrpPojoRef.getName();

        ResponseContext context = makeResourceGroupContext(
            ApiOperation.makeCreateOperation(),
            rscGrpNameStr
        );

        try
        {
            requireRscGrpMapChangeAccess();

            if (rscGrpPojoRef.getAutoSelectFilter() != null &&
                    rscGrpPojoRef.getAutoSelectFilter().getReplicaCount() != null &&
                    rscGrpPojoRef.getAutoSelectFilter().getReplicaCount() < 1)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_PLACE_COUNT,
                        getRscGrpDescription(rscGrpNameStr) + ", place-count must be positive.",
                        true
                    )
                );
            }

            ResourceGroup rscGrp = createResourceGroup(rscGrpPojoRef);

            List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
            prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.RESOURCE_DEFINITION,
                rscGrpPojoRef.getProps(),
                rscGrp.getProps(peerAccCtx.get()),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP,
                prefixesIgnoringWhitelistCheck
            );

            List<VolumeGroup> createdVlmGrps = ctrlVlmGrpApiCallHandler.createVlmGrps(
                responses,
                rscGrp,
                rscGrpPojoRef.getVlmGrpList()
            );

            resourceGroupRepository.put(apiCtx, rscGrp);

            ctrlTransactionHelper.commit();

            for (VolumeGroup vlmGrp : createdVlmGrps)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(ApiConsts.MASK_VLM_GRP | ApiConsts.CREATED);
                String successMessage = String.format(
                    "Volume group with number '%d' successfully " +
                        " created in resource group '%s'.",
                    vlmGrp.getVolumeNumber().value,
                    rscGrpNameStr
                );
                volSuccessEntry.setMessage(successMessage);
                volSuccessEntry.putObjRef(ApiConsts.KEY_RSC_GRP, rscGrpNameStr);
                volSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmGrp.getVolumeNumber().value));

                responses.addEntry(volSuccessEntry);

                errorReporter.logInfo(successMessage);
            }

            addUnknownStoragePoolWarning(rscGrp, responses);

            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultCreatedEntry(
                    rscGrp.getUuid(), getRscGrpDescriptionInline(rscGrp)
                )
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void addUnknownStoragePoolWarning(ResourceGroup rscGrpRef, ApiCallRcImpl responsesRef)
        throws AccessDeniedException
    {
        List<String> spNameDiskfulButDisklessExpectedList = new ArrayList<>();
        List<String> spNameDisklessButDiskfulExpectedList = new ArrayList<>();
        List<String> unknownSpNameList = new ArrayList<>();

        StorPoolDefinitionMap spdMap = spdRepo.getMapForView(apiCtx);
        AutoSelectorConfig autoPlaceConfig = rscGrpRef.getAutoPlaceConfig();
        for (boolean diskfulExpected : new Boolean[] {true, false})
        {
            List<String> storPoolNamesToCheck;
            if (diskfulExpected)
            {
                storPoolNamesToCheck = autoPlaceConfig.getStorPoolNameList(apiCtx);
            }
            else
            {
                storPoolNamesToCheck = autoPlaceConfig.getStorPoolDisklessNameList(apiCtx);
            }
            for (String spnameToCheck : storPoolNamesToCheck)
            {
                StorPoolDefinition spdfn = null;
                for (Entry<StorPoolName, StorPoolDefinition> entry : spdMap.entrySet())
                {
                    if (entry.getKey().value.equalsIgnoreCase(spnameToCheck))
                    {
                        spdfn = entry.getValue();
                        break;
                    }
                }
                if (spdfn != null)
                {
                    if (
                        !spdfn.streamStorPools(apiCtx)
                            .anyMatch(sp -> sp.getDeviceProviderKind().hasBackingDevice() == diskfulExpected)
                    )
                    {
                        if (diskfulExpected)
                        {
                            spNameDisklessButDiskfulExpectedList.add(spnameToCheck);
                        }
                        else
                        {
                            spNameDiskfulButDisklessExpectedList.add(spnameToCheck);
                        }
                    }
                }
                else
                {
                    unknownSpNameList.add(spnameToCheck);
                }
            }
        }


        if (
            !spNameDiskfulButDisklessExpectedList.isEmpty() ||
            !spNameDisklessButDiskfulExpectedList.isEmpty() ||
            !unknownSpNameList.isEmpty()
        )
        {
            StringBuilder sb = new StringBuilder();
            if (!spNameDiskfulButDisklessExpectedList.isEmpty())
            {
                sb.append(
                    "The following configured storage pools are diskful, but are wrongly configured to be used " +
                        "for diskless autoplacements:\n\t"
                );
                sb.append(StringUtils.join(spNameDiskfulButDisklessExpectedList, "\n\t"));
                sb.append("\n");
            }
            if (!spNameDisklessButDiskfulExpectedList.isEmpty())
            {
                sb.append(
                    "The following configured storage pools are diskless, but are wrongly configured to be used " +
                        "for diskful autoplacements:\n\t"
                );
                sb.append(StringUtils.join(spNameDisklessButDiskfulExpectedList, "\n\t"));
                sb.append("\n");
            }
            if (!unknownSpNameList.isEmpty())
            {
                sb.append(
                    "The following configured storage pools do not exist on any node:\n\t"
                );
                sb.append(StringUtils.join(unknownSpNameList, "\n\t"));
            }

            responsesRef.addEntries(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.WARN_NOT_FOUND,
                    sb.toString().trim()
                )
            );

        }

    }

    public Flux<ApiCallRc> modify(
        String rscGrpNameStrRef,
        String descriptionRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeysRef,
        HashSet<String> deleteNamespacesRef,
        AutoSelectFilterApi autoApiRef,
        @Nullable Short peerSlotsRef
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_GRP, rscGrpNameStrRef);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            getRscGrpDescription(rscGrpNameStrRef),
            getRscGrpDescriptionInline(rscGrpNameStrRef),
            ApiConsts.MASK_RSC_GRP,
            objRefs
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify resource-group",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP, RSC_GRP_MAP
                ),
                () -> modifyInTransaction(
                    rscGrpNameStrRef,
                    descriptionRef,
                    overrideProps,
                    deletePropKeysRef,
                    deleteNamespacesRef,
                    autoApiRef,
                    peerSlotsRef,
                    context
                ),
                MDC.getCopyOfContextMap()
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        String rscGrpNameStrRef,
        String descriptionRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeysRef,
        HashSet<String> deleteNamespacesRef,
        AutoSelectFilterApi autoApiRef,
        @Nullable Short peerSlotsRef,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> reRunAutoPlace = Flux.empty();
        Flux<ApiCallRc> updateStlts = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts = false;

        try
        {
            requireRscGrpMapChangeAccess();

            if (autoApiRef != null && autoApiRef.getReplicaCount() != null &&
                    autoApiRef.getReplicaCount() < 1)
            {
                throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                                ApiConsts.FAIL_INVLD_PLACE_COUNT,
                                getRscGrpDescription(rscGrpNameStrRef) + ", place-count must be positive.",
                            true
                        )
                );
            }

            ResourceGroup rscGrpData = ctrlApiDataLoader.loadResourceGroup(rscGrpNameStrRef, true);

            AccessContext peerCtx = peerAccCtx.get();
            if (descriptionRef != null)
            {
                rscGrpData.setDescription(peerCtx, descriptionRef);
            }

            if (overrideProps.containsKey(ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_EXACT_SIZE))
            {
                // currently we can only whitelist props for RD _and_ RG combined. in this case however, we only want to
                // allow this property on RD level.
                // FIXME: this should change with the properties rework
                throw new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key")
                        .setCause(
                            "The key '" + ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_EXACT_SIZE +
                                "' is not whitelisted."
                        )
                        .build()
                );
            }

            if (!overrideProps.isEmpty() || !deletePropKeysRef.isEmpty() || !deleteNamespacesRef.isEmpty())
            {
                List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
                prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

                List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
                Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilder.get()
                    .buildPropsChangedListeners(peerCtx, rscGrpData, specialPropFluxes);

                Props rscGrpProps = rscGrpData.getProps(peerCtx);
                notifyStlts = ctrlPropsHelper.fillProperties(
                    apiCallRcs,
                    LinStorObject.RESOURCE_DEFINITION,
                    overrideProps,
                    rscGrpProps,
                    ApiConsts.FAIL_ACC_DENIED_RSC_GRP,
                    prefixesIgnoringWhitelistCheck,
                    propsChangedListeners
                ) || notifyStlts;
                notifyStlts = ctrlPropsHelper.remove(
                    apiCallRcs,
                    LinStorObject.RESOURCE_DEFINITION,
                    rscGrpProps,
                    deletePropKeysRef,
                    deleteNamespacesRef,
                    prefixesIgnoringWhitelistCheck,
                    propsChangedListeners
                ) || notifyStlts;

                reRunAutoPlace = reRunAutoPlace.concatWith(
                    // TODO convert these handlers to propChangedListeners
                    handleChangedProperties(rscGrpData, overrideProps, deletePropKeysRef, deleteNamespacesRef)
                ).concatWith(Flux.merge(specialPropFluxes));
            }

            if (autoApiRef != null)
            {
                AutoSelectorConfig autoPlaceConfig = rscGrpData.getAutoPlaceConfig();
                Integer newReplicaCount = autoApiRef.getReplicaCount();
                autoPlaceConfig.applyChanges(autoApiRef);

                if (newReplicaCount != null)
                {
                    notifyStlts = true;
                    for (ResourceDefinition rscDfn : rscGrpData.getRscDfns(peerCtx))
                    {
                        long rscCount = ResourceDefinitionUtils.getResourceCount(
                            apiCtx,
                            rscDfn,
                            rsc -> !rsc.getStateFlags().isSet(apiCtx, Resource.Flags.TIE_BREAKER)
                        );
                        if (rscCount < newReplicaCount)
                        {
                            errorReporter.logDebug(
                                "Increasing replica count of resource %s from %d to %d",
                                rscDfn.getName().displayValue,
                                rscCount,
                                newReplicaCount
                            );
                            reRunAutoPlace = reRunAutoPlace.concatWith(
                                ctrlRscAutoPlaceApiCallHandler.autoPlace(
                                    rscDfn.getName().displayValue,
                                    autoApiRef
                                )
                            );
                        }
                        else
                        {
                            errorReporter.logDebug(
                                "Not changing replica count of resource %s as current resource count (%d) " +
                                    " is more than new replica count (%d)",
                                    rscDfn.getName().displayValue,
                                    rscCount,
                                    newReplicaCount
                                );
                        }
                    }
                }
                addUnknownStoragePoolWarning(rscGrpData, apiCallRcs);
            }

            if (peerSlotsRef != null)
            {
                rscGrpData.setPeerSlots(peerCtx, peerSlotsRef);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                apiCallRcs,
                context,
                ApiSuccessUtils.defaultModifiedEntry(
                    rscGrpData.getUuid(), getRscGrpDescriptionInline(rscGrpData)
                )
            );

            if (notifyStlts)
            {
                for (ResourceDefinition rscDfn : rscGrpData.getRscDfns(peerCtx))
                {
                    updateStlts = updateStlts.concatWith(
                        ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                            .transform(
                                updateResponses -> CtrlResponseUtils.combineResponses(
                                    errorReporter,
                                    updateResponses,
                                    rscDfn.getName(),
                                    "Updated Resource definition {1} on {0}"
                                )
                            )
                    );
                }
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.<ApiCallRc>just(apiCallRcs)
            .concatWith(reRunAutoPlace)
            .concatWith(updateStlts);
    }

    private Flux<ApiCallRc> handleChangedProperties(
        ResourceGroup rscGrp,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespacesRef
    )
        throws AccessDeniedException
    {
        Flux<ApiCallRc> retFlux = Flux.empty();
        String autoDiskfulKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_DISKFUL;
        if (
            overrideProps.containsKey(autoDiskfulKey) || deletePropKeys.contains(autoDiskfulKey) ||
                deletePropNamespacesRef.contains(ApiConsts.NAMESPC_DRBD_OPTIONS)
        )
        {
            autoDiskfulTask.update(rscGrp);
        }

        retFlux = retFlux.concatWith(
            ResourceDefinitionUtils.handleAutoSnapProps(
                autoSnapshotTask,
                ctrlSnapDeleteHandler,
                overrideProps,
                deletePropKeys,
                deletePropNamespacesRef,
                rscGrp.getRscDfns(peerAccCtx.get()),
                peerAccCtx.get(),
                systemConfRepository.getStltConfForView(peerAccCtx.get()),
                false
            )
        );

        for (ResourceDefinition rscDfn : rscGrp.getRscDfns(peerAccCtx.get()))
        {
            ResponseContext context = CtrlRscDfnApiCallHandler.makeResourceDefinitionContext(
                ApiOperation.makeModifyOperation(),
                rscDfn.getName().displayValue
            );

            // run auto quorum/tiebreaker manage code
            String autoTiebreakerKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
            String autoQuorumKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_QUORUM;
            if (overrideProps.containsKey(autoTiebreakerKey) || deletePropKeys.contains(autoTiebreakerKey)
                || overrideProps.containsKey(autoQuorumKey) || deletePropKeys.contains(autoQuorumKey))
            {
                ApiCallRcImpl responses = new ApiCallRcImpl();
                CtrlRscAutoHelper.AutoHelperContext autoHelperCtx = new CtrlRscAutoHelper.AutoHelperContext(responses, context, rscDfn);
                ctrlRscAutoTiebreakerHelper.manage(autoHelperCtx);
                ctrlRscAutoQuorumHelper.manage(autoHelperCtx);

                retFlux = retFlux.concatWith(Flux.merge(autoHelperCtx.additionalFluxList));
            }
        }
        return retFlux;
    }

    public ApiCallRc delete(String rscGrpNameStrRef)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        ResponseContext context = makeResourceGroupContext(
            ApiOperation.makeCreateOperation(),
            rscGrpNameStrRef
        );

        try
        {
            requireRscGrpMapChangeAccess();
            ResourceGroup rscGrpData = ctrlApiDataLoader.loadResourceGroup(
                LinstorParsingUtils.asRscGrpName(rscGrpNameStrRef),
                false
            );
            if (rscGrpData == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.WARN_NOT_FOUND,
                        getRscGrpDescription(rscGrpNameStrRef) + " not found."
                    )
                );
            }

            if (rscGrpData.hasResourceDefinitions(apiCtx))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_EXISTS_RSC_DFN,
                        "Cannot delete " + getRscGrpDescriptionInline(rscGrpNameStrRef) +
                        " because it has existing resource definitions.",
                        true
                    )
                );
            }

            UUID rscGrpUuid = rscGrpData.getUuid();
            ResourceGroupName rscGrpName = rscGrpData.getName();
            String rscGrpDisplayValue = rscGrpName.displayValue;
            deleteResourceGroup(rscGrpData);

            resourceGroupRepository.remove(apiCtx, rscGrpName);

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultDeletedEntry(
                rscGrpUuid, getRscGrpDescriptionInline(rscGrpDisplayValue)
            )
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }
        return responses;
    }

    static ResponseContext makeResourceGroupContext(
        ApiOperation operation,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_GRP, rscNameStr);

        return new ResponseContext(
            operation,
            getRscGrpDescription(rscNameStr),
            getRscGrpDescriptionInline(rscNameStr),
            ApiConsts.MASK_RSC_GRP,
            objRefs
        );
    }

    private void requireRscGrpMapChangeAccess()
    {
        try
        {
            resourceGroupRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any resource groups",
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
    }

    private ResourceGroup createResourceGroup(RscGrpPojo rscGrpPojoRef)
    {
        AutoSelectFilterApi autoSelectFilter = rscGrpPojoRef.getAutoSelectFilter();

        ResourceGroup rscGrp;
        try
        {
            List<String> nodeNameList = null;
            Integer replicaCount = null;
            List<String> storPoolNameList = null;
            List<String> storPoolDisklessNameList = null;
            List<String> doNotPlaceWithRscList = null;
            String doNotPlaceWithRscRegex = null;
            List<DeviceLayerKind> layerStackList = null;
            List<String> replicasOnSameList = null;
            List<String> replicasOnDifferentList = null;
            Map<String, Integer> xReplicasOnDifferentMap = null;
            List<DeviceProviderKind> providerList = null;
            Boolean disklessOnRemaining = null;

            if (autoSelectFilter != null)
            {
                replicaCount = autoSelectFilter.getReplicaCount();
                nodeNameList = autoSelectFilter.getNodeNameList();
                storPoolNameList = autoSelectFilter.getStorPoolNameList();
                storPoolDisklessNameList = autoSelectFilter.getStorPoolDisklessNameList();
                doNotPlaceWithRscList = autoSelectFilter.getDoNotPlaceWithRscList();
                doNotPlaceWithRscRegex = autoSelectFilter.getDoNotPlaceWithRscRegex();
                replicasOnSameList = autoSelectFilter.getReplicasOnSameList();
                layerStackList = autoSelectFilter.getLayerStackList();
                replicasOnDifferentList = autoSelectFilter.getReplicasOnDifferentList();
                xReplicasOnDifferentMap = autoSelectFilter.getXReplicasOnDifferentMap();
                providerList = autoSelectFilter.getProviderList();
                disklessOnRemaining = autoSelectFilter.getDisklessOnRemaining();
            }
            rscGrp = rscGrpFactory.create(
                peerAccCtx.get(),
                LinstorParsingUtils.asRscGrpName(rscGrpPojoRef.getName()),
                rscGrpPojoRef.getDescription(),
                layerStackList,
                replicaCount,
                nodeNameList,
                storPoolNameList,
                storPoolDisklessNameList,
                doNotPlaceWithRscList,
                doNotPlaceWithRscRegex,
                replicasOnSameList,
                replicasOnDifferentList,
                xReplicasOnDifferentMap,
                providerList,
                disklessOnRemaining,
                rscGrpPojoRef.getPeerSlots()
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "register the " + getRscGrpDescriptionInline(rscGrpPojoRef.getName()),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC_GRP,
                "A " + getRscGrpDescription(rscGrpPojoRef) + " already exists.",
                true
            ), dataAlreadyExistsExc);
        }
        return rscGrp;
    }

    private void deleteResourceGroup(ResourceGroup rscGrpData)
    {
        try
        {
            rscGrpData.delete(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getRscGrpDescriptionInline(rscGrpData),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
    }

    public Flux<ApiCallRc> spawn(
        String rscGrpNameRef,
        String rscDfnNameRef,
        byte[] rscDfnExtNameRef,
        List<Long> vlmSizesRef,
        AutoSelectFilterApi spawnAutoSelectFilterRef,
        boolean partialRef,
        boolean definitionsOnlyRef,
        @Nullable Short peerSlotsRef,
        @Nullable List<String> volumePassphrases,
        Map<String, String> rscDfnPropsRef
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_GRP, rscGrpNameRef);
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscDfnNameRef);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            getRscGrpDescription(rscGrpNameRef),
            getRscGrpDescriptionInline(rscGrpNameRef),
            ApiConsts.MASK_RSC_GRP,
            objRefs
        );

        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet()).flatMapMany(
            // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
            // the freeCapacities parameter here
            ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                "Spawn resource-definition",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP, RSC_GRP_MAP
                ),
                () -> spawnInTransaction(
                    rscGrpNameRef,
                    rscDfnNameRef,
                    rscDfnExtNameRef,
                    vlmSizesRef,
                    spawnAutoSelectFilterRef,
                    partialRef,
                    definitionsOnlyRef,
                    peerSlotsRef,
                    volumePassphrases,
                    rscDfnPropsRef,
                    context
                )
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> spawnInTransaction(
        String rscGrpNameRef,
        String rscDfnNameRef,
        byte[] rscDfnExtNameRef,
        List<Long> vlmSizesRef,
        AutoSelectFilterApi spawnAutoSelectFilterRef,
        boolean partialRef,
        boolean definitionsOnlyRef,
        @Nullable Short peerSlotsRef,
        @Nullable List<String> volumePassphrases,
        Map<String, String> rscDfnPropsRef,
        ResponseContext contextRef
    )
    {
        Flux<ApiCallRc> deployedResources = Flux.empty();
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameRef, true);
            List<String> layerStackStr;
            List<DeviceLayerKind> layerStackDevLayerKind;

            AutoSelectorConfig rgAutoPlaceConfig = rscGrp.getAutoPlaceConfig();
            AutoSelectFilterPojo autoPlaceConfig = AutoSelectFilterPojo.merge(
                spawnAutoSelectFilterRef,
                rgAutoPlaceConfig == null ? null : rgAutoPlaceConfig.getApiData()
            );

            AccessContext peerCtx = peerAccCtx.get();
            layerStackDevLayerKind = autoPlaceConfig.getLayerStackList();
            layerStackStr = new ArrayList<>();
            if (layerStackDevLayerKind != null)
            {
                for (DeviceLayerKind kind : layerStackDevLayerKind)
                {
                    layerStackStr.add(kind.toString());
                }
            }

            List<VolumeDefinitionWithCreationPayload> vlmDfnCrtList = new ArrayList<>();

            List<VolumeGroup> vlmGrps = rscGrp.getVolumeGroups(peerCtx);

            // if resource-group doesn't have any volume groups automatically switch on partial mode
            if (vlmGrps.isEmpty() && !partialRef)
            {
                apiCallRc.addEntries(ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.MASK_INFO | ApiConsts.MASK_RSC_GRP,
                    "Resource-group doesn't have any volume-groups, automatically assume partial mode."));
            }

            final boolean isPartial = vlmGrps.isEmpty() || partialRef;
            final int vlmSizeLen = vlmSizesRef.size();
            final int vlmGrpLen = vlmGrps.size();

            if (volumePassphrases != null && !volumePassphrases.isEmpty() && vlmSizeLen != volumePassphrases.size())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_REQUEST,
                            "Volume passphrase count doesn't match specified volume count."
                        )
                        .setCorrection(
                            "Please provide the same amount of volume passphrases as volumes for this resource-group")
                        .setSkipErrorReport(true));
            }

            if (vlmSizeLen == vlmGrpLen || isPartial)
            {
                for (int idx = 0; idx < vlmSizeLen; ++idx)
                {
                    Integer vlmNr = null;
                    long flags = 0;
                    if (idx < vlmGrpLen)
                    {
                        VolumeGroup vlmGrp = vlmGrps.get(idx);
                        vlmNr = vlmGrp.getVolumeNumber().value;
                        flags = vlmGrp.getFlags().getFlagsBits(peerCtx);
                    }

                    long vlmSize = vlmSizesRef.get(idx);
                    @Nullable String clearPassphrase = volumePassphrases != null && idx < volumePassphrases.size() ?
                        volumePassphrases.get(idx) : null;
                    // store passphrase encrypted in properties
                    @Nullable String encPassphrase = clearPassphrase != null ?
                        Base64.encode(encryptionHelper.encrypt(clearPassphrase)) : null;

                    vlmDfnCrtList.add(
                        createVlmDfnWithCreationPayload(
                            vlmNr,
                            vlmSize,
                            flags,
                            encPassphrase
                        )
                    );
                }
            }
            else
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_VLM_SIZES,
                        "Invalid count of volume sizes to spawn " + getRscGrpDescriptionInline(rscGrpNameRef)
                    )
                    .setDetails("The " + getRscGrpDescriptionInline(rscGrpNameRef) + " has " + vlmGrps.size() +
                        " Volume groups, but only " + vlmSizeLen + " sizes were provided.")
                    .setCorrection("Either provide the correct count of volume sizes or use the 'partial' option")
                    .setSkipErrorReport(true)
                );
            }

            LayerPayload layerPayload = new LayerPayload();
            layerPayload.getDrbdRscDfn().peerSlotsNewResource = peerSlotsRef != null ?
                peerSlotsRef :
                rscGrp.getPeerSlots(peerCtx);
            ResourceDefinition rscDfn = ctrlRscDfnApiCallHandler.createResourceDefinition(
                rscDfnNameRef,
                rscDfnExtNameRef,
                rscDfnPropsRef,
                vlmDfnCrtList,
                layerStackStr,
                layerPayload,
                rscGrpNameRef,
                true,
                apiCallRc,
                definitionsOnlyRef
            );

            if (autoPlaceConfig.getReplicaCount() != null && !definitionsOnlyRef)
            {
                AutoSelectFilterApi autoSelectFilterPojo = new AutoSelectFilterBuilder(autoPlaceConfig)
                    .setLayerStackList(layerStackDevLayerKind)
                    .build();
                deployedResources = ctrlRscAutoPlaceApiCallHandler.autoPlaceInTransaction(
                    /*
                     * do not use rscDfnNameRef here as the actual name of the rscDfn might have been
                     * generated based on the rscDfnExtNameRef
                     */
                    rscDfn.getName().displayValue,
                    autoSelectFilterPojo,
                    contextRef
                );
            }
            else
            {
                String reason = "";
                if (autoPlaceConfig.getReplicaCount() == null)
                {
                    reason = "No autoplace configuration in the given resource group (at least place-count required)";
                }
                if (definitionsOnlyRef)
                {
                    if (autoPlaceConfig.getReplicaCount() == null)
                    {
                        reason += " and ";
                    }
                    reason += "--definitions-only was set";
                }

                String actualRscDfnName = rscDfn.getName().displayValue;
                if (rscDfnExtNameRef != null)
                {
                    actualRscDfnName += "(" + new String(rscDfnExtNameRef) + ")";
                }

                deployedResources = Flux.<ApiCallRc>just(
                    new ApiCallRcImpl(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.INFO_NO_RSC_SPAWNED,
                            "Resource definition " + actualRscDfnName + " and " +
                                vlmDfnCrtList.size() +
                                " created but no resources deployed",
                                reason
                        )
                    )
                );
            }
            deployedResources = deployedResources.concatWith(
                ResourceDefinitionUtils.handleAutoSnapProps(
                    autoSnapshotTask,
                    ctrlSnapDeleteHandler,
                    Collections.emptyMap(),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    Collections.singletonList(rscDfn),
                    peerAccCtx.get(),
                    systemConfRepository.getStltConfForView(peerAccCtx.get()),
                    true
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "spawn new resource definition from " + getRscGrpDescriptionInline(rscGrpNameRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        catch (LinStorException exc)
        {
            throw new ApiException(exc);
        }
        return Flux
            .<ApiCallRc>just(apiCallRc)
            .concatWith(deployedResources);

    }

    private VolumeDefinitionWithCreationPayload createVlmDfnWithCreationPayload(
        Integer vlmNr,
        long vlmSizeRef,
        long vlmGrpFlags,
        @Nullable String encryptedPassphrase
    )
    {
        var props = new HashMap<String, String>();
        if (encryptedPassphrase != null)
        {
            props.put(ApiConsts.NAMESPC_ENCRYPTION + "/" + ApiConsts.KEY_PASSPHRASE, encryptedPassphrase);
        }
        return new VlmDfnWithCreationPayloadPojo(
            new VlmDfnPojo(
                null,
                vlmNr,
                vlmSizeRef,
                FlagsHelper.fromStringList(
                    VolumeDefinition.Flags.class,
                    FlagsHelper.toStringList(VolumeGroup.Flags.class, vlmGrpFlags)
                ),
                props,
                Collections.emptyList()
            ),
            null
        );
    }

    public Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSize(String rscGrpNameRef)
    {
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
            .flatMapMany(
                thinFreeCapacities -> scopeRunner
                    .fluxInTransactionlessScope(
                        "Query max volume size",
                        lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.STOR_POOL_DFN_MAP),
                        () -> queryMaxVlmSizeInScope(rscGrpNameRef, thinFreeCapacities),
                        MDC.getCopyOfContextMap()
                    )
            );
    }

    private Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> queryMaxVlmSizeInScope(
        String rscGrpNameRef,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        Flux<ApiCallRcWith<List<MaxVlmSizeCandidatePojo>>> flux;
        try
        {
            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameRef, true);
            AutoSelectorConfig selectFilter = rscGrp.getAutoPlaceConfig();

            AccessContext accCtx = peerAccCtx.get();

            if (selectFilter.getReplicaCount(accCtx) == null)
            {
                flux = Flux.error(
                    new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_PLACE_COUNT,
                            "Replica count is required for this operation",
                            true
                        )
                    )
                );
            }
            else
            {
                flux = qmvsHelper.queryMaxVlmSize(selectFilter.getApiData(), null, 0, thinFreeCapacities);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "no auto-place configured in " + getRscGrpDescriptionInline(rscGrpNameRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        return flux;
    }

    public Flux<ApiCallRcWith<QuerySizeInfoResponsePojo>> querySizeInfo(QuerySizeInfoRequestPojo querySizeInfoReqRef)
    {
        Flux<ApiCallRcWith<QuerySizeInfoResponsePojo>> ret;
        Pair<ApiCallRcWith<QuerySizeInfoResponsePojo>, Double> cachedResponsePair = qsiHelper.getQsiResponse(
            querySizeInfoReqRef
        );
        if (cachedResponsePair != null)
        {
            errorReporter.logDebug("Returning %.3fs old answer from cache", cachedResponsePair.objB);
            ret = Flux.just(cachedResponsePair.objA);
        }
        else
        {
            ret = freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
                .flatMapMany(
                    thinFreeCapacities -> scopeRunner
                        .fluxInTransactionlessScope(
                            "Query size info",
                            lockGuardFactory.buildDeferred(
                                LockType.WRITE,
                                LockObj.NODES_MAP,
                                LockObj.STOR_POOL_DFN_MAP
                            ),
                            () -> querySizeInfoInTransaction(querySizeInfoReqRef, thinFreeCapacities),
                            MDC.getCopyOfContextMap()
                        )
                );
        }
        return ret;
    }

    public Flux<ApiCallRcWith<QuerySizeInfoResponsePojo>> querySizeInfoInTransaction(
        QuerySizeInfoRequestPojo querySizeInfoReqRef,
        Map<StorPool.Key, Long> thinFreeCapacities
    )
    {
        Flux<ApiCallRcWith<QuerySizeInfoResponsePojo>> flux;
        try
        {
            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(querySizeInfoReqRef.getRscGrpName(), true);
            AutoSelectFilterPojo autoSelectFilterData = querySizeInfoReqRef.getAutoSelectFilterData();
            flux = Flux.just(
                querySizeInfoImpl(thinFreeCapacities, rscGrp, autoSelectFilterData)
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "querying size info for " + getRscGrpDescriptionInline(querySizeInfoReqRef.getRscGrpName()),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        return flux;
    }

    private ApiCallRcWith<QuerySizeInfoResponsePojo> querySizeInfoImpl(
        Map<StorPool.Key, Long> thinFreeCapacities,
        ResourceGroup rscGrp,
        AutoSelectFilterPojo autoSelectFilterData
    )
        throws AccessDeniedException
    {
        ApiCallRcWith<QuerySizeInfoResponsePojo> result;
        AutoSelectorConfig selectFilter = rscGrp.getAutoPlaceConfig();
        AutoSelectFilterPojo selectCfg = AutoSelectFilterPojo.merge(
            autoSelectFilterData,
            selectFilter.getApiData()
        );

        AccessContext accCtx = peerAccCtx.get();

        if (selectFilter.getReplicaCount(accCtx) == null)
        {
            result = new ApiCallRcWith<>(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_INVLD_PLACE_COUNT,
                    "Replica count is required for this operation"
                ),
                null
            );
        }
        else
        {
            result = new ApiCallRcWith<>(
                new ApiCallRcImpl(),
                qsiHelper.queryMaxVlmSize(selectCfg, thinFreeCapacities)
            );
        }
        qsiHelper.cache(rscGrp.getName().value, autoSelectFilterData, result);
        return result;
    }

    public Flux<QueryAllSizeInfoResponsePojo> queryAllSizeInfo(QueryAllSizeInfoRequestPojo queryAllSizeInfoReqRef)
    {
        Flux<QueryAllSizeInfoResponsePojo> ret;
        Pair<QueryAllSizeInfoResponsePojo, Double> cachedResponsePair = qsiHelper.getQasiResponse(
            queryAllSizeInfoReqRef
        );
        if (cachedResponsePair != null)
        {
            errorReporter.logDebug("Returning %.3fs old answer from cache", cachedResponsePair.objB);
            ret = Flux.just(cachedResponsePair.objA);
        }
        else
        {
            ret = freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
                .flatMapMany(
                    thinFreeCapacities -> scopeRunner
                        .fluxInTransactionlessScope(
                            "Query all size info",
                            lockGuardFactory.buildDeferred(
                                LockType.WRITE,
                                LockObj.NODES_MAP,
                                LockObj.STOR_POOL_DFN_MAP
                            ),
                            () -> queryAllSizeInfoInTransaction(thinFreeCapacities, queryAllSizeInfoReqRef),
                            MDC.getCopyOfContextMap()
                        )
                );
        }

        return ret;
    }

    private Flux<QueryAllSizeInfoResponsePojo> queryAllSizeInfoInTransaction(
        Map<Key, Long> thinFreeCapacitiesRef,
        QueryAllSizeInfoRequestPojo queryAllSizeInfoReqRef
    )
    {
        ApiCallRcImpl apiCallRcOuterImpl = new ApiCallRcImpl();
        Map<String, QueryAllSizeInfoResponsePojo.QueryAllSizeInfoResponseEntryPojo> map = new TreeMap<>();
        try
        {
            requireRscGrpMapChangeAccess();
            AutoSelectFilterPojo autoSelectFilterData = queryAllSizeInfoReqRef.getAutoSelectFilterData();
            for (ResourceGroup rscGrp : resourceGroupRepository.getMapForView(peerAccCtx.get()).values())
            {
                ApiCallRcWith<QuerySizeInfoResponsePojo> result = querySizeInfoImpl(
                    thinFreeCapacitiesRef,
                    rscGrp,
                    autoSelectFilterData
                );
                ApiCallRcImpl apiCallRcInnerImpl = new ApiCallRcImpl();

                QuerySizeInfoResponsePojo querySizeInfoResponsePojo = result.extractApiCallRc(apiCallRcInnerImpl);

                map.put(
                    rscGrp.getName().displayValue,
                    new QueryAllSizeInfoResponsePojo.QueryAllSizeInfoResponseEntryPojo(
                        querySizeInfoResponsePojo,
                        apiCallRcInnerImpl
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "querying all size info",
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        QueryAllSizeInfoResponsePojo response = new QueryAllSizeInfoResponsePojo(map, apiCallRcOuterImpl);
        qsiHelper.cache(queryAllSizeInfoReqRef, response);
        return Flux.just(response);
    }

    public Flux<ApiCallRc> adjust(
        String rscGrpNameRef,
        AutoSelectFilterApi adjustAutoSelectFilterRef
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_GRP, rscGrpNameRef);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            getRscGrpDescription(rscGrpNameRef),
            getRscGrpDescriptionInline(rscGrpNameRef),
            ApiConsts.MASK_RSC_GRP,
            objRefs
        );

        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet()).flatMapMany(
            // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
            // the freeCapacities parameter here
            ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                "Adjust resource-group",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP,
                    RSC_DFN_MAP,
                    STOR_POOL_DFN_MAP,
                    RSC_GRP_MAP
                ),
                () -> adjustInTransaction(
                    rscGrpNameRef,
                    adjustAutoSelectFilterRef,
                    context
                )
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    public Flux<ApiCallRc> adjustAll(AutoSelectFilterApi adjustAutoSelectFilterRef)
    {
        Map<String, String> objRefs = new TreeMap<>();

        ResponseContext context = new ResponseContext(
            ApiOperation.makeRegisterOperation(),
            "All resource groups",
            "all resource groups",
            ApiConsts.MASK_RSC_GRP,
            objRefs
        );

        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet()).flatMapMany(
            // fetchThinFreeCapacities also updates the freeSpaceManager. we can safely ignore
            // the freeCapacities parameter here
            ignoredFreeCapacities -> scopeRunner.fluxInTransactionalScope(
                "Adjust all resource-group",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP,
                    RSC_DFN_MAP,
                    STOR_POOL_DFN_MAP,
                    RSC_GRP_MAP
                ),
                () -> adjustAllInTransaction(
                    adjustAutoSelectFilterRef,
                    context
                )
            )
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> adjustAllInTransaction(
        AutoSelectFilterApi adjustAutoSelectFilterRef,
        ResponseContext contextRef
    )
    {
        Flux<ApiCallRc> ret = Flux.empty();
        try
        {
            for (ResourceGroup rg : resourceGroupRepository.getMapForView(peerAccCtx.get()).values())
            {
                ret = ret.concatWith(
                    adjustInTransaction(rg.getName().displayValue, adjustAutoSelectFilterRef, contextRef)
                );
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "iterate through resource groups",
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        return ret;
    }

    private Flux<ApiCallRc> adjustInTransaction(
        String rscGrpNameRef,
        AutoSelectFilterApi adjustAutoSelectFilterRef,
        ResponseContext contextRef
    )
    {
        List<Flux<ApiCallRc>> fluxList = new ArrayList<>();
        try
        {
            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameRef, true);

            AutoSelectorConfig rgAutoPlaceConfig = rscGrp.getAutoPlaceConfig();
            AutoSelectFilterPojo autoPlaceConfig = AutoSelectFilterPojo.merge(
                adjustAutoSelectFilterRef,
                rgAutoPlaceConfig == null ? null : rgAutoPlaceConfig.getApiData()
            );

            for (ResourceDefinition rscDfn : rscGrp.getRscDfns(peerAccCtx.get()))
            {
                fluxList.add(
                    ctrlRscAutoPlaceApiCallHandler.autoPlaceInTransaction(
                        rscDfn.getName().displayValue,
                        autoPlaceConfig,
                        contextRef
                    )
                );
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access resource group " + getRscGrpDescriptionInline(rscGrpNameRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        return Flux.merge(fluxList);
    }

    static VolumeNumber getVlmNr(VolumeGroupApi vlmGrpApi, ResourceGroup rscGrp, AccessContext accCtx)
        throws LinStorException, ValueOutOfRangeException
    {
        VolumeNumber vlmNr;
        Integer vlmNrRaw = vlmGrpApi.getVolumeNr();
        if (vlmNrRaw == null)
        {
            vlmNr = getGeneratedVlmNr(rscGrp, accCtx);
        }
        else
        {
            vlmNr = new VolumeNumber(vlmNrRaw);
        }
        return vlmNr;
    }

    static VolumeNumber getGeneratedVlmNr(ResourceGroup rscGrp, AccessContext accCtx)
        throws ImplementationError, LinStorException
    {
        VolumeNumber vlmNr;
        try
        {
            // Avoid using volume numbers that are already in use by active volumes.

            int[] occupiedVlmNrs = rscGrp.streamVolumeGroups(accCtx)
                .map(VolumeGroup::getVolumeNumber)
                .mapToInt(VolumeNumber::getValue)
                .sorted()
                .distinct()
                .toArray();

            vlmNr = VolumeNumberAlloc.getFreeVolumeNumber(occupiedVlmNrs);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(
                "ApiCtx does not have enough privileges to iterate vlmDfns",
                accDeniedExc
            );
        }
        catch (ExhaustedPoolException exhausedPoolExc)
        {
            throw new LinStorException(
                "No more free volume numbers left in range " + VolumeNumber.VOLUME_NR_MIN + " - " +
                VolumeNumber.VOLUME_NR_MAX,
                exhausedPoolExc
            );
        }
        return vlmNr;
    }

    public static String getRscGrpDescription(ResourceGroup rscGrp)
    {
        return getRscGrpDescription(rscGrp.getName().displayValue);
    }

    public static String getRscGrpDescription(RscGrpPojo rscGrpPojo)
    {
        return getRscGrpDescription(rscGrpPojo.getName());
    }

    public static String getRscGrpDescription(String rscGrpNameStr)
    {
        return "Resource group: " + rscGrpNameStr;
    }

    public static String getRscGrpDescriptionInline(ResourceGroup rscGrp)
    {
        return getRscGrpDescriptionInline(rscGrp.getName().displayValue);
    }

    public static String getRscGrpDescriptionInline(RscGrpPojo rscGrpPojo)
    {
        return getRscGrpDescriptionInline(rscGrpPojo.getName());
    }

    public static String getRscGrpDescriptionInline(String rscGrpNameStr)
    {
        return "resource group '" + rscGrpNameStr + "'";
    }
}
