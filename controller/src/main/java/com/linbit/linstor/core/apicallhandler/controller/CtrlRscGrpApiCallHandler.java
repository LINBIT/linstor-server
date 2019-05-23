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
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.pojo.VlmDfnPojo;
import com.linbit.linstor.api.pojo.VlmDfnWithCreationPayloadPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.AutoSelectorConfigData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.core.objects.ResourceGroupDataControllerFactory;
import com.linbit.linstor.core.objects.VolumeDefinition.VlmDfnWtihCreationPayload;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.VolumeGroupData;
import com.linbit.linstor.core.objects.ResourceGroup.RscGrpApi;
import com.linbit.linstor.core.objects.VolumeGroup.VlmGrpApi;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_GRP_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.google.inject.Provider;
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
    private final ResourceGroupDataControllerFactory rscGrpFactory;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final CtrlVlmGrpApiCallHandler ctrlVlmGrpApiCallHandler;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;

    private final CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandler;
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;

    @Inject
    public CtrlRscGrpApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        ScopeRunner scopeRunnerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ResourceGroupRepository rscGrpRepoRef,
        ResourceGroupDataControllerFactory rscGrpFactoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        CtrlVlmGrpApiCallHandler ctrlVlmGrpApiCallHandlerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        CtrlRscDfnApiCallHandler ctrlRscDfnApiCallHandlerRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef,
        LockGuardFactory lockGuardFactoryRef
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
        ctrlRscDfnApiCallHandler = ctrlRscDfnApiCallHandlerRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
        lockGuardFactory = lockGuardFactoryRef;
    }

    public ArrayList<RscGrpApi> listResourceGroups()
    {
        ArrayList<ResourceGroup.RscGrpApi> rscGrps = new ArrayList<>();
        try
        {
            for (ResourceGroup rscGrp : resourceGroupRepository.getMapForView(peerAccCtx.get()).values())
            {
                try
                {
                    rscGrps.add(rscGrp.getApiData(peerAccCtx.get()));
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add resource group without access
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return rscGrps;
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
            ResourceGroupData rscGrp = createResourceGroup(rscGrpPojoRef);

            ctrlPropsHelper.fillProperties(
                LinStorObject.RESOURCE_DEFINITION,
                rscGrpPojoRef.getRcsDfnProps(),
                rscGrp.getRscDfnGrpProps(peerAccCtx.get()),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );

            List<VolumeGroupData> createdVlmGrps = ctrlVlmGrpApiCallHandler.createVlmGrps(
                rscGrp,
                rscGrpPojoRef.getVlmGrpList()
            );

            resourceGroupRepository.put(apiCtx, rscGrp.getName(), rscGrp);

            ctrlTransactionHelper.commit();

            for (VolumeGroupData vlmGrp : createdVlmGrps)
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

    public Flux<ApiCallRc> modify(
        String rscGrpNameStrRef,
        String descriptionRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeysRef,
        HashSet<String> deleteNamespacesRef,
        AutoSelectFilterApi autoApiRef
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
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    public Flux<ApiCallRc> modifyInTransaction(
        String rscGrpNameStrRef,
        String descriptionRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeysRef,
        HashSet<String> deleteNamespacesRef,
        AutoSelectFilterApi autoApiRef,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> reRunAutoPlace = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();

        try
        {
            requireRscGrpMapChangeAccess();

            ResourceGroupData rscGrpData = ctrlApiDataLoader.loadResourceGroup(rscGrpNameStrRef, true);

            AccessContext peerCtx = peerAccCtx.get();
            if (descriptionRef != null)
            {
                rscGrpData.setDescription(peerCtx, descriptionRef);
            }

            if (!overrideProps.isEmpty() || !deletePropKeysRef.isEmpty() || !deleteNamespacesRef.isEmpty())
            {
                Props rscDfnGrpProps = rscGrpData.getRscDfnGrpProps(peerCtx);
                ctrlPropsHelper.fillProperties(
                    LinStorObject.RESOURCE_DEFINITION,
                    overrideProps,
                    rscDfnGrpProps,
                    ApiConsts.FAIL_ACC_DENIED_RSC_GRP
                );
                ctrlPropsHelper.remove(
                    rscDfnGrpProps,
                    deletePropKeysRef,
                    deleteNamespacesRef
                );
            }

            if (autoApiRef != null)
            {
                AutoSelectorConfigData autoPlaceConfig =
                    (AutoSelectorConfigData) rscGrpData.getAutoPlaceConfig();
                Integer newReplicaCount = autoApiRef.getReplicaCount();
                autoPlaceConfig.applyChanges(autoApiRef);

                if (newReplicaCount != null)
                {
                    List<String> layerListStr = new ArrayList<>();
                    for (DeviceLayerKind kind : autoPlaceConfig.getLayerStackList(peerCtx))
                    {
                        layerListStr.add(kind.toString());
                    }

                    for (ResourceDefinition rscDfn : rscGrpData.getRscDfns(peerCtx))
                    {
                        int rscCount = rscDfn.getResourceCount();
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
                                    autoApiRef,
                                 // autoPlaceConfig.disklessOnRemaining might be null
                                    Boolean.TRUE.equals(autoPlaceConfig.getDisklessOnRemaining(peerCtx)),
                                    layerListStr
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
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                apiCallRcs,
                context,
                ApiSuccessUtils.defaultModifiedEntry(
                    rscGrpData.getUuid(), getRscGrpDescriptionInline(rscGrpData)
                )
            );

            for (ResourceDefinition rscDfn : rscGrpData.getRscDfns(peerCtx))
            {
                responseConverter.addWithDetail(
                    apiCallRcs,
                    context,
                    ctrlSatelliteUpdater.updateSatellites(rscDfn)
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.<ApiCallRc>just(apiCallRcs)
            .concatWith(reRunAutoPlace);
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
            ResourceGroupData rscGrpData = ctrlApiDataLoader.loadResourceGroup(
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
                        " because it has existing resource definitions."
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

    private ResourceGroupData createResourceGroup(RscGrpPojo rscGrpPojoRef)
    {
        AutoSelectFilterApi autoSelectFilter = rscGrpPojoRef.getAutoSelectFilter();

        ResourceGroupData rscGrp;
        try
        {
            Integer replicaCount = null;
            String storPoolNameStr = null;
            List<String> doNotPlaceWithRscList = null;
            String doNotPlaceWithRscRegex = null;
            List<DeviceLayerKind> layerStackList = null;
            List<String> replicasOnSameList = null;
            List<String> replicasOnDifferentList = null;
            List<DeviceProviderKind> providerList = null;
            Boolean disklessOnRemaining = null;

            if (autoSelectFilter != null)
            {
                replicaCount = autoSelectFilter.getReplicaCount();
                storPoolNameStr = autoSelectFilter.getStorPoolNameStr();
                doNotPlaceWithRscList = autoSelectFilter.getDoNotPlaceWithRscList();
                doNotPlaceWithRscRegex = autoSelectFilter.getDoNotPlaceWithRscRegex();
                replicasOnSameList = autoSelectFilter.getReplicasOnSameList();
                layerStackList = autoSelectFilter.getLayerStackList();
                replicasOnDifferentList = autoSelectFilter.getReplicasOnDifferentList();
                providerList = autoSelectFilter.getProviderList();
                disklessOnRemaining = autoSelectFilter.getDisklessOnRemaining();
            }
            rscGrp = rscGrpFactory.create(
                peerAccCtx.get(),
                LinstorParsingUtils.asRscGrpName(rscGrpPojoRef.getName()),
                rscGrpPojoRef.getDescription(),
                layerStackList,
                replicaCount,
                storPoolNameStr,
                doNotPlaceWithRscList,
                doNotPlaceWithRscRegex,
                replicasOnSameList,
                replicasOnDifferentList,
                providerList,
                disklessOnRemaining
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
                "A " + getRscGrpDescription(rscGrpPojoRef) + " already exists."
            ), dataAlreadyExistsExc);
        }
        return rscGrp;
    }

    private void deleteResourceGroup(ResourceGroupData rscGrpData)
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
        List<Long> vlmSizesRef,
        boolean partialRef
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

        return scopeRunner
            .fluxInTransactionalScope(
                "Spawn resource-definition",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP, RSC_GRP_MAP
                ),
                () -> spawnInTransaction(
                    rscGrpNameRef,
                    rscDfnNameRef,
                    vlmSizesRef,
                    partialRef,
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> spawnInTransaction(
        String rscGrpNameRef,
        String rscDfnNameRef,
        List<Long> vlmSizesRef,
        boolean partialRef,
        ResponseContext contextRef
    )
    {
        Flux<ApiCallRc> deployedResources;
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        try
        {
            ResourceGroupData rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameRef, true);
            List<String> layerStackStr;
            List<DeviceLayerKind> layerStackDevLayerKind;

            AutoSelectorConfig autoPlaceConfig = rscGrp.getAutoPlaceConfig();
            AccessContext peerCtx = peerAccCtx.get();
            if (autoPlaceConfig != null)
            {
                layerStackDevLayerKind = autoPlaceConfig.getLayerStackList(peerCtx);
                layerStackStr = new ArrayList<>();
                for (DeviceLayerKind kind : layerStackDevLayerKind)
                {
                    layerStackStr.add(kind.toString());
                }
            }
            else
            {
                layerStackDevLayerKind = Collections.emptyList();
                layerStackStr = Collections.emptyList();
            }

            List<VlmDfnWtihCreationPayload> vlmDfnCrtList = new ArrayList<>();

            List<VolumeGroup> vlmGrps = rscGrp.getVolumeGroups(peerCtx);
            final int vlmSizeLen = vlmSizesRef.size();
            final int vlmGrpLen = vlmGrps.size();
            if (vlmSizeLen == vlmGrps.size() || partialRef)
            {
                for (int idx = 0; idx < vlmSizeLen; ++idx)
                {
                    long vlmSize = vlmSizesRef.get(idx).longValue();
                    Integer vlmNr = null;
                    if (idx < vlmGrpLen)
                    {
                        vlmNr = vlmGrps.get(idx).getVolumeNumber().value;
                    }
                    vlmDfnCrtList.add(
                        createVlmDfnWithCreationPayload(
                            vlmNr,
                            vlmSize
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
                );
            }

            apiCallRc.addEntries(
                ctrlRscDfnApiCallHandler.createResourceDefinition(
                    rscDfnNameRef,
                    null,
                    null,
                    null,
                    null,
                    rscGrp.getRscDfnGrpProps(peerCtx).map(),
                    vlmDfnCrtList,
                    layerStackStr,
                    null,
                    rscGrpNameRef
                )
            );

            if (autoPlaceConfig != null)
            {
                AutoSelectFilterApi autoSelectFilterPojo = new AutoSelectFilterPojo(
                    autoPlaceConfig.getReplicaCount(peerCtx),
                    autoPlaceConfig.getStorPoolNameStr(peerCtx),
                    autoPlaceConfig.getDoNotPlaceWithRscList(peerCtx),
                    autoPlaceConfig.getDoNotPlaceWithRscRegex(peerCtx),
                    autoPlaceConfig.getReplicasOnSameList(peerCtx),
                    autoPlaceConfig.getReplicasOnDifferentList(peerCtx),
                    layerStackDevLayerKind,
                    autoPlaceConfig.getProviderList(peerCtx),
                    autoPlaceConfig.getDisklessOnRemaining(peerCtx)
                );
                deployedResources = ctrlRscAutoPlaceApiCallHandler.autoPlaceInTransaction(
                    rscDfnNameRef,
                    autoSelectFilterPojo,
                    false,
                    contextRef,
                    layerStackStr
                );
            }
            else
            {
                deployedResources = Flux.empty();
            }

        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "spawn new resource definition from " + getRscGrpDescriptionInline(rscGrpNameRef),
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
            );
        }
        return Flux
            .<ApiCallRc>just(apiCallRc)
            .concatWith(deployedResources);

    }

    private VlmDfnWtihCreationPayload createVlmDfnWithCreationPayload(Integer vlmNr, long vlmSizeRef)
    {
        return new VlmDfnWithCreationPayloadPojo(
            new VlmDfnPojo(
                null,
                vlmNr,
                vlmSizeRef,
                0,
                Collections.emptyMap(),
                Collections.emptyList()
            ),
            null
        );
    }

    static VolumeNumber getVlmNr(VlmGrpApi vlmGrpApi, ResourceGroup rscGrp, AccessContext accCtx)
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
            vlmNr = new VolumeNumber(vlmNrRaw.intValue());
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
