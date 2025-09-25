package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.VolumeNumberAlloc;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.BackupInfoManager;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMapExtName;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDefinitionUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.SatelliteResourceStateDrbdUtils;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWithCreationPayload;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceControllerFactory;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionControllerFactory;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.core.types.NodeId;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.debug.HexViewer;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.layer.utils.SuspendLayerUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.tasks.AutoDiskfulTask;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.utils.Base64;
import com.linbit.utils.CollectionUtils;
import com.linbit.utils.StringUtils;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.helpers.ExternalNameConverter.createResourceName;
import static com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller.notConnectedError;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.MDC;
import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDfnApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlVlmDfnApiCallHandler vlmDfnHandler;
    private final CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelper;
    private final CtrlVlmCrtApiHelper ctrlVlmCrtApiHelper;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceGroupControllerFactory resourceGroupFactory;
    private final ResourceDefinitionControllerFactory resourceDefinitionFactory;
    private final ResourceGroupRepository resourceGroupRepository;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final ResponseConverter responseConverter;
    private final CtrlSecurityObjects secObjs;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscLayerDataFactory ctrlLayerStackHelper;
    private final EncryptionHelper encHelper;
    private final AutoSnapshotTask autoSnapshotTask;
    private final AutoDiskfulTask autoDiskfulTask;
    private final CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandler;
    private final CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandler;
    private final CtrlRscCrtApiHelper ctrlRscCrtApiHelper;
    private final CtrlRscAutoHelper ctrlRscAutoHelper;
    private final FreeCapacityFetcher freeCapacityFetcher;
    private final ResourceControllerFactory resourceControllerFactory;
    private final BackupInfoManager backupInfoMgr;
    private final SystemConfRepository systemConfRepository;
    private final CtrlRscDfnApiCallHelper ctrlRscDfnApiCallHelper;
    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilder;
    private final Autoplacer autoplacer;

    @Inject
    public CtrlRscDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlVlmDfnApiCallHandler vlmDfnHandlerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceGroupControllerFactory resourceGroupFactoryRef,
        ResourceDefinitionControllerFactory resourceDefinitionFactoryRef,
        ResourceGroupRepository resourceGroupRepositoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ResponseConverter responseConverterRef,
        CtrlSecurityObjects secObjsRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlRscLayerDataFactory ctrlLayerStackHelperRef,
        EncryptionHelper encHelperRef,
        AutoSnapshotTask autoSnapshotTaskRef,
        AutoDiskfulTask autoDiskfulTaskRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandlerRef,
        CtrlRscAutoPlaceApiCallHandler ctrlRscAutoPlaceApiCallHandlerRef,
        CtrlVlmDfnCrtApiHelper ctrlVlmDfnCrtApiHelperRef,
        CtrlRscCrtApiHelper ctrlRscCrtApiHelperRef,
        CtrlRscAutoHelper ctrlRscAutoHelperRef,
        FreeCapacityFetcher freeCapacityFetcherRef,
        ResourceControllerFactory resourceControllerFactoryRef,
        CtrlVlmCrtApiHelper ctrlVlmCrtApiHelperRef,
        BackupInfoManager backupInfoMgrRef,
        SystemConfRepository systemConfRepositoryRef,
        CtrlRscDfnApiCallHelper ctrlRscDfnApiCallHelperRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderRef,
        Autoplacer autoplacerRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        vlmDfnHandler = vlmDfnHandlerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceGroupFactory = resourceGroupFactoryRef;
        resourceDefinitionFactory = resourceDefinitionFactoryRef;
        resourceGroupRepository = resourceGroupRepositoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        responseConverter = responseConverterRef;
        secObjs = secObjsRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
        ctrlLayerStackHelper = ctrlLayerStackHelperRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        encHelper = encHelperRef;
        autoSnapshotTask = autoSnapshotTaskRef;
        autoDiskfulTask = autoDiskfulTaskRef;
        ctrlSnapDeleteHandler = ctrlSnapDeleteHandlerRef;
        ctrlRscAutoPlaceApiCallHandler = ctrlRscAutoPlaceApiCallHandlerRef;
        ctrlVlmDfnCrtApiHelper = ctrlVlmDfnCrtApiHelperRef;
        ctrlRscCrtApiHelper = ctrlRscCrtApiHelperRef;
        ctrlRscAutoHelper = ctrlRscAutoHelperRef;
        freeCapacityFetcher = freeCapacityFetcherRef;
        resourceControllerFactory = resourceControllerFactoryRef;
        ctrlVlmCrtApiHelper = ctrlVlmCrtApiHelperRef;
        backupInfoMgr = backupInfoMgrRef;
        systemConfRepository = systemConfRepositoryRef;
        ctrlRscDfnApiCallHelper = ctrlRscDfnApiCallHelperRef;
        propsChangeListenerBuilder = propsChangeListenerBuilderRef;
        autoplacer = autoplacerRef;
    }

    public @Nullable ResourceDefinition createResourceDefinition(
        String rscNameStr,
        @Nullable byte[] extName,
        Map<String, String> props,
        List<VolumeDefinitionWithCreationPayload> volDescrMap,
        List<String> layerStackStrList,
        @Nullable LayerPayload payloadPrm,
        @Nullable String rscGrpNameStr,
        boolean throwOnError,
        ApiCallRcImpl apiCallRc,
        boolean commit
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeCreateOperation(),
            rscNameStr
        );
        ResourceDefinition rscDfn = null;

        LayerPayload payload = payloadPrm;
        if (payload == null)
        {
            payload = new LayerPayload();
        }

        try
        {
            requireRscDfnMapChangeAccess();

            List<DeviceLayerKind> layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStrList);
            if (!layerStack.isEmpty())
            {
                if (layerStack.contains(DeviceLayerKind.LUKS))
                {
                    warnIfMasterKeyIsNotSet(responses);
                }
                if (!layerStack.get(layerStack.size() - 1).equals(DeviceLayerKind.STORAGE))
                {
                    warnAddedStorageLayer(responses);
                    layerStack.add(DeviceLayerKind.STORAGE);
                }
            }

            rscDfn = createRscDfn(
                rscNameStr,
                extName,
                layerStack,
                payload,
                rscGrpNameStr
            );

            if (rscNameStr.trim().isEmpty())
            {
                // an external name was given which means that we have to update the object-references
                // so the response of this create API is correctly filled
                context.getObjRefs().put(ApiConsts.KEY_RSC_DFN, rscDfn.getName().displayValue);
            }

            List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
            prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.RSC_DFN,
                props,
                ctrlPropsHelper.getProps(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN,
                prefixesIgnoringWhitelistCheck
            );

            List<VolumeDefinition> createdVlmDfns = vlmDfnHandler.createVlmDfns(responses, rscDfn, volDescrMap);

            resourceDefinitionRepository.put(apiCtx, rscDfn);

            if (commit)
            {
                ctrlTransactionHelper.commit();
            }
            errorReporter.logInfo("Resource definition created %s", rscNameStr);

            for (VolumeDefinition vlmDfn : createdVlmDfns)
            {
                ApiCallRcEntry volSuccessEntry = new ApiCallRcEntry();
                volSuccessEntry.setReturnCode(ApiConsts.MASK_VLM_DFN | ApiConsts.CREATED);
                String successMessage = String.format(
                    "Volume definition with number '%d' successfully " +
                        " created in resource definition '%s'.",
                    vlmDfn.getVolumeNumber().value,
                    rscNameStr
                );
                volSuccessEntry.setMessage(successMessage);
                volSuccessEntry.putObjRef(ApiConsts.KEY_RSC_DFN, rscNameStr);
                volSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmDfn.getVolumeNumber().value));

                responses.addEntry(volSuccessEntry);

                errorReporter.logInfo(successMessage);
            }

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultCreatedEntry(
                rscDfn.getUuid(), getRscDfnDescriptionInline(rscDfn)));
        }
        catch (Exception | ImplementationError exc)
        {
            if (throwOnError)
            {
                if (exc instanceof ApiRcException)
                {
                    throw (ApiRcException) exc;
                }
                else
                {
                    throw new ApiException(exc);
                }
            }
            else
            {
                responses = responseConverter.reportException(peer.get(), context, exc);
            }
        }

        apiCallRc.addEntries(responses);
        return rscDfn;
    }

    private void warnIfMasterKeyIsNotSet(ApiCallRcImpl responsesRef) throws AccessDeniedException
    {
        byte[] masterKey = secObjs.getCryptKey();
        if ((masterKey == null || masterKey.length == 0))
        {
            boolean passphraseExists = encHelper.passphraseExists(peerAccCtx.get());

            String warnMsg = "The master key has not yet been set. Creating volume definitions within \n" +
                "an encrypted resource definition will fail!";

            errorReporter.logWarning(warnMsg);

            responsesRef.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.WARN_NOT_FOUND_CRYPT_KEY,
                    warnMsg
                )
                    .setCorrection(
                        (passphraseExists ? "Enter " : "Create ") +
                            " the master passphrase, or remove the luks layer from the stack"
                    )
                .build()
            );
        }
    }

    private void warnAddedStorageLayer(ApiCallRcImpl responsesRef)
    {
        String warnMsg = "The layerstack was extended with STORAGE kind.";
        errorReporter.logWarning(warnMsg);

        responsesRef.addEntry(
            ApiCallRcImpl.entryBuilder(
                ApiConsts.WARN_STORAGE_KIND_ADDED,
                warnMsg
            )
            .setDetails("Layer stacks have to be based on STORAGE kind. Layers configured to be diskless\n" +
                "will not use the additional STORAGE layer.")
            .build()
        );
    }

    public Flux<ApiCallRc> modify(
        @Nullable UUID rscDfnUuid,
        String rscNameStr,
        @Nullable Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces,
        List<String> layerStackStrList,
        @Nullable Short newRscPeerSlots,
        @Nullable String rscGroupName
    )
    {
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeModifyOperation(),
            rscNameStr
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify resource-definition",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP, RSC_DFN_MAP
                ),
                () -> modifyInTransaction(
                    rscDfnUuid,
                    rscNameStr,
                    portInt,
                    overrideProps,
                    deletePropKeys,
                    deleteNamespaces,
                    layerStackStrList,
                    newRscPeerSlots,
                    rscGroupName,
                    context
                ),
                MDC.getCopyOfContextMap()
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        @Nullable UUID rscDfnUuid,
        String rscNameStr,
        @Nullable Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        List<String> layerStackStrList,
        @Nullable Short newRscPeerSlots,
        @Nullable String rscGroupName,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        Flux<ApiCallRc> autoFlux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts = false;

        try
        {
            requireRscDfnMapChangeAccess();

            ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
            if (backupInfoMgr.restoreContainsRscDfn(rscDfn))
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_IN_USE,
                        rscNameStr + " is currently being restored from a backup. " +
                            "Please wait until the restore is finished"
                    )
                );
            }
            if (rscDfnUuid != null && !rscDfnUuid.equals(rscDfn.getUuid()))
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(
                        ApiConsts.FAIL_UUID_RSC_DFN,
                        "UUID-check failed"
                    )
                    .setDetails("local UUID: " + rscDfn.getUuid().toString() +
                        ", received UUID: " + rscDfnUuid.toString())
                    .build()
                );
            }
            if (portInt != null || newRscPeerSlots != null)
            {
                notifyStlts = true;
                // TODO: might be a good idea to create this object earlier
                LayerPayload payload = new LayerPayload();
                payload.getDrbdRscDfn().tcpPort = portInt;
                payload.getDrbdRscDfn().peerSlotsNewResource = newRscPeerSlots;
                ctrlLayerStackHelper.ensureRequiredRscDfnLayerDataExits(
                    rscDfn,
                    "",
                    payload
                );
            }

            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty() || !deletePropNamespaces.isEmpty())
            {
                Props rscDfnProps = ctrlPropsHelper.getProps(rscDfn);

                List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
                prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

                List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
                Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilder.get()
                    .buildPropsChangedListeners(peerAccCtx.get(), rscDfn, specialPropFluxes);

                // ExactSize check
                @Nullable String exactSizeValue = overrideProps.get(
                    ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_EXACT_SIZE
                );
                if (Boolean.parseBoolean(exactSizeValue) && rscDfn.getResourceCount() > 0)
                {
                    String msg = String.format(
                        "The property '%s/%s' cannot be set to True while there are currently deployed resources(%d).",
                        ApiConsts.NAMESPC_DRBD_OPTIONS,
                        ApiConsts.KEY_DRBD_EXACT_SIZE,
                        rscDfn.getResourceCount());
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_PROP,
                            msg,
                            true
                        )
                    );
                }

                notifyStlts = ctrlPropsHelper.fillProperties(
                    apiCallRcs,
                    LinStorObject.RSC_DFN,
                    overrideProps,
                    rscDfnProps,
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN,
                    prefixesIgnoringWhitelistCheck,
                    propsChangedListeners
                ) || notifyStlts;
                notifyStlts = ctrlPropsHelper.remove(
                    apiCallRcs,
                    LinStorObject.RSC_DFN,
                    rscDfnProps,
                    deletePropKeys,
                    deletePropNamespaces,
                    prefixesIgnoringWhitelistCheck,
                    propsChangedListeners
                ) || notifyStlts;

                autoFlux = autoFlux.concatWith(
                    // TODO convert these handlers to propChangedListeners
                    handleChangedProperties(
                        rscDfn,
                        overrideProps,
                        deletePropKeys,
                        deletePropNamespaces,
                        apiCallRcs,
                        context
                    )
                ).concatWith(Flux.merge(specialPropFluxes));

            }

            if (!layerStackStrList.isEmpty())
            {
                List<DeviceLayerKind> layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStrList);

                if (!layerStack.equals(rscDfn.getLayerStack(peerAccCtx.get())) && rscDfn.getResourceCount() > 0)
                {
                    throw new ApiRcException(ApiCallRcImpl
                        .entryBuilder(
                            ApiConsts.FAIL_EXISTS_RSC,
                            "Changing the layer stack with already deployed resource is not supported"
                        )
                        .build()
                    );
                }
                notifyStlts = true;
                rscDfn.setLayerStack(peerAccCtx.get(), layerStack);
            }

            if (rscGroupName != null &&
                !rscDfn.getResourceGroup().getName().getName().equalsIgnoreCase(rscGroupName))
            {
                ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGroupName, true);

                int curDiskfulRscCount = getDiskfulResourceCount(rscDfn);
                int newReplCount = getReplicaCount(rscGrp);

                if (curDiskfulRscCount < newReplCount)
                {
                    autoFlux = autoFlux.concatWith(
                        ctrlRscAutoPlaceApiCallHandler.autoPlace(
                            rscDfn.getName().displayValue,
                            rscGrp.getAutoPlaceConfig().getApiData(),
                            false,
                            null
                        )
                    );
                }

                rscDfn.setResourceGroup(peerAccCtx.get(), rscGrp);
                notifyStlts = true;
            }

            ctrlTransactionHelper.commit();

            errorReporter.logInfo("Resource definition modified %s/%s", rscNameStr, notifyStlts);

            responseConverter.addWithOp(
                apiCallRcs, context,
                ApiSuccessUtils.defaultModifiedEntry(
                    rscDfn.getUuid(), getRscDfnDescriptionInline(rscDfn)
                )
            );

            if (notifyStlts)
            {
                flux = flux.concatWith(
                    ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                        .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
                        .concatWith(autoFlux)
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs).concatWith(flux);
    }

    private int getDiskfulResourceCount(ResourceDefinition rscDfnRef)
    {
        int count;
        try
        {
            count = (int) rscDfnRef.streamResource(apiCtx).filter(rsc ->
            {
                try
                {
                    return rsc.getStateFlags().isUnset(apiCtx, Resource.Flags.DISKLESS);
                }
                catch (AccessDeniedException exc)
                {
                    throw new ImplementationError(exc);
                }
            }).count();
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return count;
    }

    private int getReplicaCount(ResourceGroup rscGrp)
    {
        try
        {
            return rscGrp.getAutoPlaceConfig().getReplicaCount(apiCtx);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private Flux<ApiCallRc> handleChangedProperties(
        ResourceDefinition rscDfn,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespacesRef,
        ApiCallRcImpl responsesRef,
        ResponseContext contextRef
    ) throws AccessDeniedException
    {
        Flux<ApiCallRc> retFlux = Flux.empty();

        retFlux = retFlux.concatWith(
            ResourceDefinitionUtils.handleAutoSnapProps(
                autoSnapshotTask,
                ctrlSnapDeleteHandler,
                overrideProps,
                deletePropKeys,
                deletePropNamespacesRef,
                Collections.singletonList(rscDfn),
                peerAccCtx.get(),
                systemConfRepository.getStltConfForView(peerAccCtx.get()),
                false
            )
        );

        String drbdQuorum = ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM;
        boolean drbdQuorumChanged = false;
        if (overrideProps.containsKey(drbdQuorum))
        {
            overrideProps.put(ApiConsts.NAMESPC_INTERNAL_DRBD + "/" + ApiConsts.KEY_QUORUM_SET_BY, "user");
            drbdQuorumChanged = true;
        }

        if (deletePropKeys.contains(drbdQuorum))
        {
            deletePropKeys.add(ApiConsts.NAMESPC_INTERNAL_DRBD + "/" + ApiConsts.KEY_QUORUM_SET_BY);
            drbdQuorumChanged = true;
        }

        String autoTiebreakerKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
        if (overrideProps.containsKey(autoTiebreakerKey) || deletePropKeys.contains(autoTiebreakerKey) ||
            drbdQuorumChanged)
        {
            AutoHelperContext autoHelperCtx = new AutoHelperContext(responsesRef, contextRef, rscDfn);
            ctrlRscAutoHelper.manage(autoHelperCtx);

            retFlux = retFlux.concatWith(Flux.merge(autoHelperCtx.additionalFluxList));
        }

        String autoDiskfulKey = ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_DISKFUL;
        if (
            overrideProps.containsKey(autoDiskfulKey) || deletePropKeys.contains(autoDiskfulKey) ||
                deletePropNamespacesRef.contains(ApiConsts.NAMESPC_DRBD_OPTIONS)
        )
        {
            autoDiskfulTask.update(rscDfn);
        }

        return retFlux;
    }

    ArrayList<ResourceDefinitionApi> listResourceDefinitions(List<String> rscDfnNames, List<String> propFilters)
    {
        ArrayList<ResourceDefinitionApi> rscdfns = new ArrayList<>();
        final Set<ResourceName> rscDfnsFilter =
            rscDfnNames.stream().map(LinstorParsingUtils::asRscName).collect(Collectors.toSet());

        try
        {
            resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(
                    rscDfn ->
                    (
                        rscDfnsFilter.isEmpty() ||
                        rscDfnsFilter.contains(rscDfn.getName())
                    )
                )
                .forEach(
                    rscDfn ->
                    {
                        try
                        {
                            final ReadOnlyProps props = rscDfn.getProps(peerAccCtx.get());
                            if (props.contains(propFilters))
                            {
                                rscdfns.add(rscDfn.getApiData(peerAccCtx.get()));
                            }
                        }
                        catch (AccessDeniedException accDeniedExc)
                        {
                            // don't add resource definition without access
                        }
                    }
                );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            // for now return an empty list.
        }

        return rscdfns;
    }

    private ApiCallRc copyVlmDfn(final ResourceDefinition srcRscDfn, final ResourceDefinition destRscDfn)
        throws AccessDeniedException
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        for (VolumeDefinition srcVlmDfn :
            srcRscDfn.streamVolumeDfn(peerAccCtx.get()).collect(Collectors.toList()))
        {
            VolumeDefinition vlmDfn = ctrlVlmDfnCrtApiHelper.createVlmDfnData(
                peerAccCtx.get(),
                destRscDfn,
                srcVlmDfn.getVolumeNumber(),
                null,
                srcVlmDfn.getVolumeSize(peerAccCtx.get()),
                VolumeDefinition.Flags.restoreFlags(srcVlmDfn.getFlags().getFlagsBits(peerAccCtx.get()))
            );

            Map<String, String> srcVlmDfnProps = srcVlmDfn.getProps(peerAccCtx.get()).map();
            Map<String, String> vlmDfnProps = vlmDfn.getProps(peerAccCtx.get()).map();
            vlmDfnProps.putAll(srcVlmDfnProps);
        }

        return responses;
    }

    public Flux<ApiCallRc> cloneRscDfn(
        String srcRscName,
        String clonedRscName,
        @Nullable byte[] clonedExtName,
        @Nullable Boolean useZfsClone,
        @Nullable List<String> volumePassphrases,
        @Nullable List<String> layerList,
        @Nullable String intoRscGrpName,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces
    )
    {
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeCreateOperation(),
            clonedRscName
        );
        // TODO optimize: only fetch involved nodes
        return freeCapacityFetcher.fetchThinFreeCapacities(Collections.emptySet())
            .flatMapMany(thinFreeCapacities ->
                scopeRunner
                    .fluxInTransactionalScope(
                        "Clone resource-definition",
                        lockGuardFactory.buildDeferred(LockGuardFactory.LockType.WRITE, NODES_MAP, RSC_DFN_MAP),
                        () -> cloneRscDfnInTransaction(
                            srcRscName, clonedRscName, clonedExtName, useZfsClone,
                            volumePassphrases, layerList, context, thinFreeCapacities, intoRscGrpName,
                            overrideProps, deletePropKeys, deleteNamespaces)
                    )
                    .transform(responses -> responseConverter.reportingExceptions(context, responses)));
    }

    private Flux<ApiCallRc> setCloneSnapshotProperty(ResourceDefinition rscDfn, String cloneRscName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Set clone snapshot property",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> setCloneSnapshotPropertyInTrans(rscDfn, cloneRscName)
            );
    }

    private Flux<ApiCallRc> setCloneSnapshotPropertyInTrans(ResourceDefinition rscDfn, String cloneRscName)
    {
        try
        {
            var diskfulRscs = rscDfn.getDiskfulResources(apiCtx);
            for (var diskRsc : diskfulRscs)
            {
                setCloneSnapshotPropPrivileged(diskRsc, cloneRscName);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set clone snapshot property for " + getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller
            .updateSatellites(rscDfn, notConnectedError(), Flux.empty())
            .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    responses,
                    rscDfn.getName(),
                    "Set snapshot for clone property of {1} on {0}"
                )
            );
    }

    private void setCloneSnapshotPropPrivileged(Resource rsc, String cloneRscName)
    {
        try
        {
            Props props = rsc.getProps(apiCtx);
            props.setProp(InternalApiConsts.CLONE_PROP_PREFIX + cloneRscName, cloneRscName);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set resource clone property for " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (InvalidKeyException|InvalidValueException invKey)
        {
            errorReporter.reportError(invKey);
            throw new RuntimeException("Invalid key or value for clone snapshot property:" + cloneRscName);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void removeCloneSnapshotPropPrivileged(ResourceDefinition rscDfn, String cloneRscName)
    {
        try
        {
            Iterator<Resource> itRsc = rscDfn.iterateResource(apiCtx);
            while (itRsc.hasNext())
            {
                Resource rsc = itRsc.next();
                Props props = rsc.getProps(apiCtx);
                props.removeProp(InternalApiConsts.CLONE_PROP_PREFIX + cloneRscName);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "remove resource clone property",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (InvalidKeyException invKey)
        {
            errorReporter.reportError(invKey);
            throw new RuntimeException("Invalid key or value for clone snapshot property:" + cloneRscName);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private Flux<ApiCallRc> resumeIOAndClearCloneProp(ResourceDefinition rscDfn, String cloneName)
    {
        return scopeRunner
            .fluxInTransactionalScope(
                "Resume resource",
                lockGuardFactory.create()
                    .read(LockObj.NODES_MAP)
                    .write(LockObj.RSC_DFN_MAP)
                    .buildDeferred(),
                () -> resumeIOAndClearClonePropInTransaction(rscDfn, cloneName)
            );
    }

    private Flux<ApiCallRc> resumeIOAndClearClonePropInTransaction(ResourceDefinition rscDfn, String cloneName)
    {
        resumeIOPrivileged(rscDfn);
        removeCloneSnapshotPropPrivileged(rscDfn, cloneName);

        ctrlTransactionHelper.commit();

        return ctrlSatelliteUpdateCaller
            .updateSatellites(rscDfn, notConnectedError(), Flux.empty())
            .transform(
                responses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    responses,
                    rscDfn.getName(),
                    "Resumed IO of {1} on {0} after clone"
                )
            );
    }

    private void setSuspendIO(Resource rsc)
    {
        try
        {
            SuspendLayerUtils.suspendIo(apiCtx, rsc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "set resource suspension for " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void resumeIOPrivileged(ResourceDefinition rscDfn)
    {
        try
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(apiCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                SuspendLayerUtils.resumeIo(apiCtx, rsc);
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

    private Flux<ApiCallRc> removeStartCloning(ResourceDefinition rscDfn) {
        return scopeRunner
            .fluxInTransactionalScope(
                "Disabled start cloning flag",
                lockGuardFactory.create()
                    .write(LockObj.RSC_DFN_MAP, LockObj.NODES_MAP)
                    .buildDeferred(),
                () -> removeStartCloningInTransaction(rscDfn)
            );
    }

    private Flux<ApiCallRc> removeStartCloningInTransaction(ResourceDefinition rscDfn)
        throws AccessDeniedException, DatabaseException
    {
        Iterator<Resource> it = rscDfn.iterateResource(peerAccCtx.get());
        while (it.hasNext())
        {
            Resource rsc = it.next();

            // remove start cloning flag from vlms
            if (!rsc.isDiskless(peerAccCtx.get()))
            {
                for (Volume vlm : rsc.streamVolumes().collect(Collectors.toList()))
                {
                    vlm.getFlags().disableFlags(peerAccCtx.get(), Volume.Flags.CLONING_START);
                }
                ResourceDataUtils.recalculateVolatileRscData(ctrlLayerStackHelper, rsc);
            }
        }

        ctrlTransactionHelper.commit();
        return ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, notConnectedError(), Flux.empty())
            .transform(
                updateResponses -> CtrlResponseUtils.combineResponses(
                    errorReporter,
                    updateResponses,
                    rscDfn.getName(),
                    "Disabled start cloning flag " + rscDfn.getName()
                )
            );
    }

    private void storeVolumePassphrasesToVlmDfn(ResourceDefinition rscDfn, @Nullable List<String> volumePassphrasesRef)
        throws LinStorException, InvalidValueException
    {
        List<String> volumePassphrases = volumePassphrasesRef != null ?
            new ArrayList<>(volumePassphrasesRef) : new ArrayList<>();

        boolean hasLuks = rscDfn.usesLayer(peerAccCtx.get(), DeviceLayerKind.LUKS);
        if (volumePassphrases.isEmpty() && hasLuks)
        {
            // generate new passwords anyway
            for (int i = 0; i < rscDfn.getVolumeDfnCount(peerAccCtx.get()); i++)
            {
                volumePassphrases.add(encHelper.generateSecretString(16));
            }
        }

        if (!CollectionUtils.isEmpty(volumePassphrases))
        {
            if (rscDfn.getVolumeDfnCount(peerAccCtx.get()) != volumePassphrases.size())
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

            Iterator<VolumeDefinition> itVlmDfn = rscDfn.iterateVolumeDfn(peerAccCtx.get());
            int idx = 0;
            while (itVlmDfn.hasNext())
            {
                VolumeDefinition vlmDfn = itVlmDfn.next();

                String clearPassphrase = volumePassphrases.get(idx);
                // store passphrase encrypted in properties
                byte[] encPassphrase = encHelper.encrypt(clearPassphrase);
                vlmDfn.getProps(peerAccCtx.get())
                    .setProp(ApiConsts.NAMESPC_ENCRYPTION + "/" + ApiConsts.KEY_PASSPHRASE,
                        Base64.encode(encPassphrase));

                idx++;
            }
        }
    }

    private LayerPayload createRscDfnPayload(ResourceDefinition srcRscDfn) throws AccessDeniedException
    {
        LayerPayload payload = new LayerPayload();
        Map<String, RscDfnLayerObject> drbd = srcRscDfn.getLayerData(peerAccCtx.get(), DeviceLayerKind.DRBD);
        for (RscDfnLayerObject obj : drbd.values())
        {
            DrbdRscDfnData<Resource> drbdRscDfnData = (DrbdRscDfnData<Resource>) obj;
            payload.drbdRscDfn.peerSlotsNewResource = drbdRscDfnData.getPeerSlots();
            payload.drbdRscDfn.transportType = drbdRscDfnData.getTransportType();
            payload.drbdRscDfn.alStripes = drbdRscDfnData.getAlStripes();
            payload.drbdRscDfn.alStripeSize = drbdRscDfnData.getAlStripeSize();
        }
        return payload;
    }

    /**
     * Override resource layer data, that has to be the same as in the original resource.
     * @param origRsc
     * @param newPayload
     * @throws AccessDeniedException
     */
    private void overrideRscLayerData(Resource origRsc, LayerPayload newPayload) throws AccessDeniedException
    {
        // set same node ids as before
        Set<AbsRscLayerObject<Resource>> rscLayerSet = LayerRscUtils.getRscDataByLayer(
            origRsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD);
        for (AbsRscLayerObject<Resource> rscLayer : rscLayerSet)
        {
            DrbdRscData<Resource> drbdRscData = ((DrbdRscData<Resource>) rscLayer);
            NodeId oldNodeId = drbdRscData.getNodeId();
            newPayload.drbdRsc.nodeId = oldNodeId.value;
            newPayload.drbdRsc.peerSlots = drbdRscData.getPeerSlots();
            newPayload.drbdRsc.alStripes = drbdRscData.getAlStripes();
            newPayload.drbdRsc.alStripeSize = drbdRscData.getAlStripeSize();
        }
    }

    private static LayerPayload createVlmLayerPayload(
        Map<String, StorPool> storPool, @Nullable StorPool rscStoragePool, VolumeDefinition toVlmDfn)
    {
        LayerPayload vlmPayload = new LayerPayload();
        for (Entry<String, StorPool> entry : storPool.entrySet())
        {
            StorPool usePool = entry.getKey().equals(RscLayerSuffixes.SUFFIX_DATA) &&
                entry.getValue().getDeviceProviderKind() != DeviceProviderKind.DISKLESS &&
                rscStoragePool != null ?
                rscStoragePool : entry.getValue();
            vlmPayload.putStorageVlmPayload(
                entry.getKey(),
                toVlmDfn.getVolumeNumber().value,
                usePool
            );
        }
        return vlmPayload;
    }

    private Map<NodeName, StorPool> findCloneStoragePools(ResourceDefinition srcRscDfn, ResourceDefinition clonedRscDfn)
        throws AccessDeniedException
    {
        AutoSelectFilterPojo autoSelectFilterPojo = AutoSelectFilterPojo.copy(
            clonedRscDfn.getResourceGroup().getAutoPlaceConfig().getApiData());
        List<Resource> diskfulRscs = srcRscDfn.getDiskfulResources(peerAccCtx.get());
        autoSelectFilterPojo.setPlaceCount(diskfulRscs.size());
        autoSelectFilterPojo.setNodeNameList(diskfulRscs.stream()
            .map(diskRsc -> diskRsc.getNode().getName().displayValue).collect(Collectors.toList()));
        Set<StorPool> possibleStorPools = autoplacer.autoPlace(autoSelectFilterPojo, clonedRscDfn,
            CtrlRscAutoPlaceApiCallHandler.calculateResourceDefinitionSize(srcRscDfn, peerAccCtx.get()));

        if (possibleStorPools == null || possibleStorPools.size() != diskfulRscs.size())
        {
            throw new ApiRcException(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.FAIL_NOT_ENOUGH_NODES, "No suitable storage pools found for cloning."));
        }

        return possibleStorPools.stream().collect(
            Collectors.toMap(storPool -> storPool.getNode().getName(), storPool -> storPool));
    }

    private void failIfDrbdMetadataConversion(Resource srcRsc, Resource cloneRsc) throws AccessDeniedException
    {
        Set<AbsRscLayerObject<Resource>> srcDrbdRscSet = LayerRscUtils
            .getRscDataByLayer(srcRsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD);
        Set<AbsRscLayerObject<Resource>> cloneDrbdRscSet = LayerRscUtils
            .getRscDataByLayer(cloneRsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD);
        var itSrcSet = srcDrbdRscSet.iterator();
        var itCloneSet = cloneDrbdRscSet.iterator();
        if (itSrcSet.hasNext() && itCloneSet.hasNext())
        {
            var srcDrbdLayer = itSrcSet.next();
            var cloneDrbdLayer = itCloneSet.next();
            if (srcDrbdLayer.getChildren().size() != cloneDrbdLayer.getChildren().size())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.FAIL_INVLD_LAYER_STACK,
                        "Internal <> external metadata conversion not supported yet.",
                        true
                    ));
            }
        }
    }

    private void failIfNoName(@Nullable String clonedRscName, @Nullable byte[] clonedExtName)
    {
        if (StringUtils.isEmpty(clonedRscName) &&
            (clonedExtName == null || StringUtils.isEmpty(new String(clonedExtName))))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_RSC_NAME,
                    "No name for the cloned resource was specified.",
                    true
                ));
        }
    }

    /**
     * Check if the resource has skip disk set or if has DRBD all are upToDate.
     * @param rsc resource to check.
     * @throws AccessDeniedException
     */
    private void failIfWrongRscState(Resource rsc) throws AccessDeniedException
    {
        Props props = rsc.getProps(peerAccCtx.get());
        if (ApiConsts.VAL_TRUE.equalsIgnoreCase(
            props.getProp(ApiConsts.KEY_DRBD_SKIP_DISK, ApiConsts.NAMESPC_DRBD_OPTIONS)))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_RSC_STATE,
                    String.format("Resource %s has skip-disk set, unable to clone.", rsc),
                    true
                ));
        }

        if (!rsc.isDiskless(apiCtx) &&
            rsc.hasDrbd(apiCtx) &&
            !SatelliteResourceStateDrbdUtils.allVolumesUpToDate(apiCtx, rsc, false))
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_NOT_ALL_UPTODATE,
                    "Cannot clone resource-definition, because we have a non-UpToDate DRBD device.",
                    true
                ));
        }
    }

    public Flux<ApiCallRc> cloneRscDfnInTransaction(
        String srcRscName,
        String clonedRscName,
        byte[] clonedExtName,
        @Nullable Boolean useZfsClone,
        @Nullable List<String> volumePassphrases,
        @Nullable List<String> layerList,
        ResponseContext context,
        Map<StorPool.Key, Long> thinFreeCapacities,
        @Nullable String intoRscGrpName,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces)
    {
        Flux<ApiCallRc> flux;

        try
        {
            failIfNoName(clonedRscName, clonedExtName);
            ApiCallRcImpl responses = new ApiCallRcImpl();
            requireRscDfnMapChangeAccess();

            final ResourceDefinition srcRscDfn = ctrlApiDataLoader.loadRscDfn(srcRscName, true);

            final LayerPayload payload = createRscDfnPayload(srcRscDfn);
            final List<DeviceLayerKind> layerStack = CollectionUtils.isEmpty(layerList) ?
                srcRscDfn.getLayerStack(peerAccCtx.get()) : LinstorParsingUtils.asDeviceLayerKind(layerList);
            final String rscGrpName = intoRscGrpName != null && !intoRscGrpName.isEmpty() ?
                intoRscGrpName : srcRscDfn.getResourceGroup().getName().getName();
            final ResourceDefinition clonedRscDfn = createRscDfn(
                clonedRscName,
                clonedExtName,
                layerStack,
                payload,
                rscGrpName);

            Props clonedRscDfnProps = clonedRscDfn.getProps(peerAccCtx.get());
            Map<String, String> clonedRscDfnPropsMap = clonedRscDfnProps.map();
            clonedRscDfnPropsMap.putAll(srcRscDfn.getProps(peerAccCtx.get()).map());
            clonedRscDfnPropsMap.put(InternalApiConsts.KEY_CLONED_FROM, srcRscDfn.getName().displayValue);

            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty() || !deleteNamespaces.isEmpty())
            {
                List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
                prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

                // ExactSize check
                @Nullable String exactSizeValue = overrideProps.get(
                    ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_EXACT_SIZE
                );
                if (Boolean.parseBoolean(exactSizeValue))
                {
                    String msg = String.format(
                        "The property '%s/%s' cannot be set to True while there are currently deployed resources(%d).",
                        ApiConsts.NAMESPC_DRBD_OPTIONS,
                        ApiConsts.KEY_DRBD_EXACT_SIZE,
                        srcRscDfn.getResourceCount());
                    throw new ApiRcException(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.FAIL_INVLD_PROP,
                            msg,
                            true
                        )
                    );
                }

                ctrlPropsHelper.fillProperties(
                    responses,
                    LinStorObject.RSC_DFN,
                    overrideProps,
                    clonedRscDfnProps,
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN,
                    prefixesIgnoringWhitelistCheck
                );
                ctrlPropsHelper.remove(
                    responses,
                    LinStorObject.RSC_DFN,
                    clonedRscDfnProps,
                    deletePropKeys,
                    deleteNamespaces,
                    prefixesIgnoringWhitelistCheck
                );
            }

            final ReadOnlyProps rscGrpProps = srcRscDfn.getResourceGroup().getProps(peerAccCtx.get());
            PriorityProps rscGrpPrioProps = new PriorityProps(
                rscGrpProps, systemConfRepository.getCtrlConfForView(peerAccCtx.get())
            );
            String propUseZfsClone = rscGrpPrioProps.getProp(ApiConsts.KEY_USE_ZFS_CLONE);
            boolean finalZfsClone = useZfsClone != null ? useZfsClone : StringUtils.propTrueOrYes(propUseZfsClone);
            if (finalZfsClone)
            {
                clonedRscDfnPropsMap.put(InternalApiConsts.KEY_USE_ZFS_CLONE, "true");
            }
            clonedRscDfn.getFlags().enableFlags(peerAccCtx.get(), ResourceDefinition.Flags.CLONING);

            resourceDefinitionRepository.put(apiCtx, clonedRscDfn);

            responses.addEntries(copyVlmDfn(srcRscDfn, clonedRscDfn));

            storeVolumePassphrasesToVlmDfn(clonedRscDfn, volumePassphrases);

            Map<NodeName, StorPool> possibleStorPools = findCloneStoragePools(srcRscDfn, clonedRscDfn);

            Set<Resource> deployedResources = new TreeSet<>();
            Iterator<Resource> it = srcRscDfn.iterateResource(peerAccCtx.get());
            while (it.hasNext())
            {
                Resource rsc = it.next();

                failIfWrongRscState(rsc);

                setSuspendIO(rsc);

                overrideRscLayerData(rsc, payload);
                Resource newRsc = resourceControllerFactory.create(
                    peerAccCtx.get(),
                    clonedRscDfn,
                    rsc.getNode(),
                    payload,
                    Resource.Flags.restoreFlags(rsc.getStateFlags().getFlagsBits(peerAccCtx.get())),
                    layerStack
                );

                ctrlPropsHelper.copy(
                    ctrlPropsHelper.getProps(rsc),
                    ctrlPropsHelper.getProps(newRsc)
                );
                // overwrite storpool props
                @Nullable StorPool rscStoragePool = possibleStorPools.get(rsc.getNode().getName());
                if (rscStoragePool != null && !rsc.isDiskless(peerAccCtx.get()))
                {
                    // TODO get rid of this property in the future
                    newRsc.getProps(peerAccCtx.get()).setProp(
                        ApiConsts.KEY_STOR_POOL_NAME, rscStoragePool.getName().getDisplayName());
                }

                Iterator<VolumeDefinition> toVlmDfnIter = ctrlRscCrtApiHelper.getVlmDfnIterator(clonedRscDfn);
                while (toVlmDfnIter.hasNext())
                {
                    VolumeDefinition toVlmDfn = toVlmDfnIter.next();
                    VolumeNumber volumeNumber = toVlmDfn.getVolumeNumber();

                    Volume srcVlm = rsc.getVolume(volumeNumber);

                    Map<String, StorPool> storPool = LayerVlmUtils.getStorPoolMap(rsc, volumeNumber, peerAccCtx.get());
                    LayerPayload vlmPayload = createVlmLayerPayload(storPool, rscStoragePool, toVlmDfn);

                    Volume cloneVlm = ctrlVlmCrtApiHelper.createVolume(
                            newRsc,
                            toVlmDfn,
                            vlmPayload,
                            thinFreeCapacities,
                            Collections.emptyMap(),
                            responses
                        );

                    ctrlPropsHelper.copy(
                        ctrlPropsHelper.getProps(srcVlm),
                        ctrlPropsHelper.getProps(cloneVlm)
                    );
                }

                failIfDrbdMetadataConversion(rsc, newRsc);

                Set<AbsRscLayerObject<Resource>> rscLayerSet = LayerRscUtils.getRscDataByLayer(
                    newRsc.getLayerData(peerAccCtx.get()), DeviceLayerKind.DRBD);
                for (AbsRscLayerObject<Resource> rscLayer : rscLayerSet)
                {
                    DrbdRscData<Resource> drbdRscData = ((DrbdRscData<Resource>) rscLayer);
                    drbdRscData.getFlags().disableFlags(peerAccCtx.get(), DrbdRscObject.DrbdRscFlags.INITIALIZED);
                }

                // mark all volumes as cloning
                for (Volume vlm : newRsc.streamVolumes().collect(Collectors.toList()))
                {
                    if (!newRsc.isDiskless(peerAccCtx.get()))
                    {
                        Set<StorPool> storPools = LayerVlmUtils.getStorPoolSet(vlm, peerAccCtx.get(), false);
                        boolean allSupportCloning = storPools.stream()
                            .allMatch(storPool -> storPool.getDeviceProviderKind().isCloneSupported());

                        if (!allSupportCloning)
                        {
                            throw new ApiRcException(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.FAIL_INVLD_PROVIDER, "Clone source contains unsupported storage pools"));
                        }
                        vlm.getFlags().enableFlags(peerAccCtx.get(), Volume.Flags.CLONING_START, Volume.Flags.CLONING);
                    }
                    else
                    {
                        vlm.getFlags().enableFlags(peerAccCtx.get(), Volume.Flags.CLONING_FINISHED);
                    }
                }

                ResourceDataUtils.recalculateVolatileRscData(ctrlLayerStackHelper, rsc);
                ResourceDataUtils.recalculateVolatileRscData(ctrlLayerStackHelper, newRsc);
                deployedResources.add(newRsc);
            }

            ctrlTransactionHelper.commit();

            for (Resource rsc : deployedResources)
            {
                responseConverter.addWithOp(responses, context,
                    ApiSuccessUtils.defaultRegisteredEntry(rsc.getUuid(), getRscDescriptionInline(rsc)));

                responses.addEntries(CtrlRscCrtApiCallHandler.makeVolumeRegisteredEntries(rsc));
            }

            Flux<ApiCallRc> deploymentResponses = ctrlRscCrtApiHelper
                .deployResources(context, deployedResources, false);

            flux = ctrlSatelliteUpdateCaller.updateSatellites(srcRscDfn, Flux.empty())
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        srcRscDfn.getName(),
                        "Suspend IO on clone source " + srcRscName
                    )
                )
                .concatWith(Flux.just(responses))
                .concatWith(setCloneSnapshotProperty(srcRscDfn, clonedRscName))
                .concatWith(resumeIOAndClearCloneProp(srcRscDfn, clonedRscName))
                .concatWith(deploymentResponses)
                .concatWith(removeStartCloning(clonedRscDfn))
                .onErrorResume(exception -> resumeIOAndClearCloneProp(srcRscDfn, clonedRscName));
        }
        catch (ApiRcException exc)
        {
            throw exc;
        }
        catch (Exception | ImplementationError exc)
        {
            throw new ApiException(exc);
        }

        return flux;
    }

    private void requireRscDfnMapChangeAccess()
    {
        try
        {
            resourceDefinitionRepository.requireAccess(
                peerAccCtx.get(),
                AccessType.CHANGE
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "change any resource definitions",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
    }

    private ResourceDefinition createRscDfn(
        String rscNameStr,
        @Nullable byte[] extNamePrm,
        List<DeviceLayerKind> layerStack,
        LayerPayload payload,
        @Nullable String rscGrpNameStrPrm
    )
        throws InvalidNameException, ValueOutOfRangeException
    {
        if (rscNameStr == null)
        {
            throw new ImplementationError("Resource name must not be null!");
        }

        boolean generatedRscName = false;
        ResourceName rscName = null;
        byte[] extName = extNamePrm;
        if (!rscNameStr.isEmpty())
        {
            // A resource name was specified, an external name may have been specified optionally
            rscName = LinstorParsingUtils.asRscName(rscNameStr);

            if (extName != null)
            {
                // throws ApiRcException if the external name is not unique
                checkExtNameUnique(extName, peerAccCtx.get(), resourceDefinitionRepository);
            }
        }
        else
        {
            generatedRscName = true;
            // The zero-length resource name was specified, the resource name will be generated
            // depending on the contents of the specified external name
            if (extName != null)
            {
                // throws ApiRcException if the external name is not unique
                checkExtNameUnique(extName, peerAccCtx.get(), resourceDefinitionRepository);

                try
                {
                    // Create a unique resource name from the specified external name
                    // Falls back to a prefix + UUID name if the resource name collides with an existing one
                    // A prefix + UUID name is also created if a zero-length external name is specified
                    rscName = createResourceName(extName, resourceDefinitionRepository.getMapForView(peerAccCtx.get()));
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    throw new ApiAccessDeniedException(
                        accDeniedExc,
                        "getMapForView / getMapForViewExtName",
                        ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                    );
                }
            }
            else
            {
                ApiCallRcImpl.EntryBuilder rcEntry = ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_MISSING_EXT_NAME,
                    "The resource name generation for the creation of a new resource definition failed"
                );
                rcEntry.setCause(
                    "Name generation was selected by specifying a zero-length resource name,\n" +
                    "but the external name field required for name generation is not present\n" +
                    "in the API call data"
                );
                rcEntry.setCorrection(
                    "For name generation from an external name\n" +
                    "- Specify a non-zero-length external name to induce resource name generation\n" +
                    "  based on the specified external name" +
                    "- Specify a zero-length external name to induce generation of a random\n" +
                    "  resource name (Prefix + UUID)\n" +
                    "or\n" +
                    "- Specify a non-zero-length resource name to avoid resource name generation"
                );
                throw new ApiRcException(rcEntry.build());
            }
        }

        // Discard zero-length external names, as those are used to trigger
        // generation of a UUID-based resource name and are not supposed to be stored
        if (extName != null && extName.length == 0)
        {
            extName = null;
        }

        if (!layerStack.isEmpty())
        {
            CtrlRscCrtApiHelper.ensureLayerStackIsAllowed(layerStack);
        }

        ResourceDefinition rscDfn;
        try
        {
            String rscGrpNameStr;
            if (rscGrpNameStrPrm == null)
            {
                rscGrpNameStr = InternalApiConsts.DEFAULT_RSC_GRP_NAME;
            }
            else
            {
                rscGrpNameStr = rscGrpNameStrPrm;
            }

            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameStr, false);
            if (rscGrp == null)
            {
                if (InternalApiConsts.DEFAULT_RSC_GRP_NAME.equalsIgnoreCase(rscGrpNameStr))
                {
                    rscGrp = resourceGroupFactory.create(
                        peerAccCtx.get(),
                        new ResourceGroupName(rscGrpNameStr),
                        "Default resource group",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                    );
                    resourceGroupRepository.put(apiCtx, rscGrp);
                }
                else
                {
                    throw new ApiRcException(
                        ApiCallRcImpl.singleApiCallRc(
                            ApiConsts.FAIL_NOT_FOUND_RSC_GRP,
                            String.format("Resource group '%s' could not be found.", rscGrpNameStr)
                        )
                    );
                }
            }

            if (payload.getDrbdRscDfn().peerSlotsNewResource == null)
            {
                payload.getDrbdRscDfn().peerSlotsNewResource = rscGrp.getPeerSlots(peerAccCtx.get());
            }

            rscDfn = resourceDefinitionFactory.create(
                peerAccCtx.get(),
                rscName,
                extName,
                null, // RscDfnFlags
                layerStack,
                payload,
                rscGrp
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {

            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getRscDfnDescriptionInline(rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            ApiCallRcImpl.EntryBuilder rcEntry = ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_EXISTS_RSC_DFN,
                "A resource definition with the name '" + rscNameStr + "' already exists"
            );
            rcEntry.setSkipErrorReport(true);
            throw new ApiRcException(rcEntry.build(), exc);
        }
        catch (LinStorException lsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_UNKNOWN_ERROR,
                    "Volume definition creation failed due to an unidentified error code, see text message " +
                    "of nested exception"
                ),
                lsExc
            );
        }
        return rscDfn;
    }

    public Flux<ApiCallRc> updateProps(ResourceDefinition rscDfn)
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscDfn.getName().displayValue);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeModifyOperation(),
            "Resource definitions for " + getRscDfnDescription(rscDfn),
            "resource definitions for " + getRscDfnDescriptionInline(rscDfn),
            ApiConsts.MASK_RSC_DFN,
            objRefs
        );
        return scopeRunner
            .fluxInTransactionalScope(
                "Update DRBD Props",
                lockGuardFactory.buildDeferred(WRITE, RSC_DFN_MAP),
                () -> updatePropsInTransaction(
                    rscDfn
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> updatePropsInTransaction(ResourceDefinition rscDfnRef)
    {
        Flux<ApiCallRc> ret = Flux.empty();
        if (!rscDfnRef.isDeleted())
        {
            boolean changed = ctrlRscDfnApiCallHelper.updateDrbdProps(rscDfnRef);
            ctrlTransactionHelper.commit();
            if (changed)
            {
                ret = ctrlSatelliteUpdateCaller.updateSatellites(rscDfnRef, Flux.empty())
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponses,
                            rscDfnRef.getName(),
                            Collections.emptyList(),
                            "Updated " + getRscDfnDescription(rscDfnRef),
                            null
                        )
                    );
            }
        }
        return ret;
    }

    static VolumeNumber getVlmNr(VolumeDefinitionApi vlmDfnApi, ResourceDefinition rscDfn, AccessContext accCtx)
        throws ValueOutOfRangeException, LinStorException
    {
        VolumeNumber vlmNr;
        Integer vlmNrRaw = vlmDfnApi.getVolumeNr();
        if (vlmNrRaw == null)
        {
            try
            {
                // Avoid using volume numbers that are already in use by active volumes or snapshots.
                // Re-using snapshot volume numbers would result in confusion when restoring from the snapshot.

                Set<SnapshotVolumeDefinition> snapshotVlmDfns = new HashSet<>();
                for (SnapshotDefinition snapshotDfn : rscDfn.getSnapshotDfns(accCtx))
                {
                    snapshotVlmDfns.addAll(snapshotDfn.getAllSnapshotVolumeDefinitions(accCtx));
                }

                int[] occupiedVlmNrs = Stream.concat(
                    rscDfn.streamVolumeDfn(accCtx).map(VolumeDefinition::getVolumeNumber),
                    snapshotVlmDfns.stream().map(SnapshotVolumeDefinition::getVolumeNumber)
                )
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
        }
        else
        {
            vlmNr = new VolumeNumber(vlmNrRaw);
        }
        return vlmNr;
    }

    public static String getRscDfnDescription(ResourceDefinition rscDfn)
    {
        return getRscDfnDescription(rscDfn.getName().displayValue);
    }

    public static String getRscDfnDescription(ResourceName rscName)
    {
        return getRscDfnDescription(rscName.displayValue);
    }

    public static String getRscDfnDescription(String rscName)
    {
        return "Resource definition: " + rscName;
    }

    public static String getRscDfnDescriptionInline(ResourceDefinition rscDfn)
    {
        return getRscDfnDescriptionInline(rscDfn.getName());
    }

    public static String getRscDfnDescriptionInline(ResourceName rscName)
    {
        return getRscDfnDescriptionInline(rscName.displayValue);
    }

    public static String getRscDfnDescriptionInline(String rscName)
    {
        return "resource definition '" + rscName + "'";
    }

    public static String getCrtRscDfnName(final ResourceName rscName, final boolean generated)
    {
        StringBuilder nameInfo = new StringBuilder();
        nameInfo.append("The name for the new resource definition was '");
        nameInfo.append(rscName.displayValue);
        nameInfo.append("'");
        if (generated)
        {
            nameInfo.append("\nThis resource name was generated from external name data");
        }
        return nameInfo.toString();
    }

    static ResponseContext makeResourceDefinitionContext(
        ApiOperation operation,
        String rscNameStr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_DFN, rscNameStr);

        return new ResponseContext(
            operation,
            getRscDfnDescription(rscNameStr),
            getRscDfnDescriptionInline(rscNameStr),
            ApiConsts.MASK_RSC_DFN,
            objRefs
        );
    }

    private static void checkExtNameUnique(
        final byte[] extName,
        final AccessContext accCtx,
        final ResourceDefinitionRepository rscDfnRps
    )
        throws ApiRcException, ApiAccessDeniedException
    {
        if (extName.length > 0)
        {
            try
            {
                // Check whether the specified external name is already registered
                ResourceDefinitionMapExtName extNameMap =
                    rscDfnRps.getMapForViewExtName(accCtx);
                if (extNameMap.containsKey(extName))
                {
                    ApiCallRcImpl.EntryBuilder errorRcBld = ApiCallRcImpl.entryBuilder(
                        ApiConsts.FAIL_EXISTS_EXT_NAME,
                        "The specified external name is already registered"
                    );
                    errorRcBld.setDetails(
                        "The external name data is:\n" +
                        HexViewer.binaryToHexDump(extName)
                    );
                    ApiCallRcEntry errorRc = errorRcBld.build();
                    throw new ApiRcException(errorRc);
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ApiAccessDeniedException(
                    accDeniedExc,
                    "getMapForView / getMapForViewExtName",
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                );
            }
        }
    }

    public Flux<ApiCallRc> setDeployFile(String rscName, String extFileName, boolean deploy)
    {
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeModifyOperation(),
            rscName
        );

        return scopeRunner.fluxInTransactionalScope(
            "Deploy external file on resource definition",
            lockGuardFactory.create().read(LockObj.EXT_FILE_MAP).write(LockObj.RSC_DFN_MAP).buildDeferred(),
            () -> deployFileInTransaction(rscName, extFileName, deploy)
        ).transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> deployFileInTransaction(
        String rscNameRef,
        String extFileNameRef,
        boolean deployRef
    )
    {
        Flux<ApiCallRc> flux;
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscNameRef, true);
        ExternalFile extFile = ctrlApiDataLoader.loadExtFile(extFileNameRef, true);

        try
        {
            if (deployRef)
            {
                CtrlExternalFilesHelper.deployPath(rscDfn.getProps(peerAccCtx.get()), extFile);
            }
            else
            {
                /*
                 * Do not use undeployPath. Using undeployPath only sets the property's value to False, which might
                 * result in conflicts where different rscDfns (or later rscDfn with node / ctrl) have different values
                 * for the same external file.
                 *
                 * Using only deployPath and removePath prevents such conflicts by defining "if anything requires the
                 * file to exist, the file will be created".
                 */
                CtrlExternalFilesHelper.removePath(rscDfn.getProps(peerAccCtx.get()), extFile);
            }

            ctrlTransactionHelper.commit();

            flux = ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                .transform(
                    updateResponses -> CtrlResponseUtils.combineResponses(
                        errorReporter,
                        updateResponses,
                        rscDfn.getName(),
                        (deployRef ? "Deployed " : "Undeployed ") + extFileNameRef + " on resource {1} on {0}"
                    )
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
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "deploy external file on " + getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }

        return flux;
    }

    public boolean isResourceSynced(String resourceName)
    {
        ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(resourceName, true);
        try
        {
            return SatelliteResourceStateDrbdUtils.allResourcesUpToDate(
                rscDfn.streamResource(peerAccCtx.get()).filter(resource -> {
                    boolean diskfull;
                    try
                    {
                        diskfull = !resource.isDiskless(peerAccCtx.get());
                    }
                    catch (AccessDeniedException ignored)
                    {
                        diskfull = true;
                    }
                    return diskfull;
                })
                    .map(AbsResource::getNode).collect(Collectors.toSet()),
                rscDfn.getName(),
                peerAccCtx.get()
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "Access denied on " + getRscDfnDescription(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
    }
}
