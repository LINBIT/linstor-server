package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.VolumeNumberAlloc;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CoreModule.ResourceDefinitionMapExtName;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.ResourceDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionApi;
import com.linbit.linstor.core.apis.VolumeDefinitionWtihCreationPayload;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionControllerFactory;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupControllerFactory;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepository;
import com.linbit.linstor.core.repository.ResourceGroupRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.debug.HexViewer;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.locks.LockGuardFactory;

import static com.linbit.linstor.core.apicallhandler.controller.helpers.ExternalNameConverter.createResourceName;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscDfnApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlVlmDfnApiCallHandler vlmDfnHandler;
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
    private final CtrlConfApiCallHandler ctrlConfApiCallHandler;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlRscLayerDataFactory ctrlLayerStackHelper;
    private final EncryptionHelper encHelper;

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
        CtrlConfApiCallHandler ctrlConfApiCallHandlerRef,
        CtrlRscLayerDataFactory ctrlLayerStackHelperRef,
        EncryptionHelper encHelperRef
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
        ctrlConfApiCallHandler = ctrlConfApiCallHandlerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        encHelper = encHelperRef;
    }

    public ResourceDefinition createResourceDefinition(
        String rscNameStr,
        byte[] extName,
        Integer portInt,
        String secret,
        String transportTypeStr,
        Map<String, String> props,
        List<VolumeDefinitionWtihCreationPayload> volDescrMap,
        List<String> layerStackStrList,
        Short peerSlotsRef,
        String rscGrpNameStr,
        boolean throwOnError,
        ApiCallRcImpl apiCallRc
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeCreateOperation(),
            rscNameStr
        );
        ResourceDefinition rscDfn = null;

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
                transportTypeStr,
                portInt,
                secret,
                layerStack,
                peerSlotsRef,
                rscGrpNameStr
            );

            if (rscNameStr.trim().isEmpty())
            {
                // an external name was given which means that we have to update the object-references
                // so the response of this create API is correctly filled
                context.getObjRefs().put(ApiConsts.KEY_RSC_DFN, rscDfn.getName().displayValue);
            }

            ctrlPropsHelper.fillProperties(responses, LinStorObject.RESOURCE_DEFINITION, props,
                ctrlPropsHelper.getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

            List<VolumeDefinition> createdVlmDfns = vlmDfnHandler.createVlmDfns(responses, rscDfn, volDescrMap);

            resourceDefinitionRepository.put(apiCtx, rscDfn);

            ctrlTransactionHelper.commit();

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
        UUID rscDfnUuid,
        String rscNameStr,
        Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces,
        List<String> layerStackStrList,
        Short newRscPeerSlots
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
                    context
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyInTransaction(
        UUID rscDfnUuid,
        String rscNameStr,
        Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespaces,
        List<String> layerStackStrList,
        Short newRscPeerSlots,
        ResponseContext context
    )
    {
        Flux<ApiCallRc> flux = Flux.empty();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();

        try
        {
            requireRscDfnMapChangeAccess();

            ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
            ResourceDefinition rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
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

            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Props rscDfnProps = ctrlPropsHelper.getProps(rscDfn);

                ctrlPropsHelper.fillProperties(
                    apiCallRcs,
                    LinStorObject.RESOURCE_DEFINITION,
                    overrideProps,
                    rscDfnProps,
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                );
                ctrlPropsHelper.remove(
                    apiCallRcs,
                    LinStorObject.RESOURCE_DEFINITION,
                    rscDfnProps,
                    deletePropKeys,
                    deletePropNamespaces
                );
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
                rscDfn.setLayerStack(peerAccCtx.get(), layerStack);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(apiCallRcs, context, ApiSuccessUtils.defaultModifiedEntry(
                rscDfn.getUuid(), getRscDfnDescriptionInline(rscDfn)));

            flux = ctrlSatelliteUpdateCaller
                .updateSatellites(rscDfn, Flux.empty())
                .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2());
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs).concatWith(flux);
    }

    ArrayList<ResourceDefinitionApi> listResourceDefinitions(List<String> rscDfnNames, List<String> propFilters)
    {
        ArrayList<ResourceDefinitionApi> rscdfns = new ArrayList<>();
        final Set<ResourceName> rscDfnsFilter =
            rscDfnNames.stream().map(LinstorParsingUtils::asRscName).collect(Collectors.toSet());

        try
        {
            resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values().stream()
                .filter(rscDfn ->
                    (
                        rscDfnsFilter.isEmpty() ||
                        rscDfnsFilter.contains(rscDfn.getName())
                    )
                )
                .forEach(rscDfn ->
                    {
                        try
                        {
                            final Props props = rscDfn.getProps(peerAccCtx.get());
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
        byte[] extNamePrm,
        String transportTypeStr,
        Integer portInt,
        String secret,
        List<DeviceLayerKind> layerStack,
        Short peerSlotsRef,
        String rscGrpNameStrPrm
    )
        throws InvalidNameException
    {
        if (rscNameStr == null)
        {
            throw new ImplementationError("Resource name must not be null!");
        }

        TransportType transportType = null;
        if (transportTypeStr != null && !transportTypeStr.trim().isEmpty())
        {
            try
            {
                transportType = TransportType.byValue(transportTypeStr); // TODO needs exception
                                                                         // handling
            }
            catch (IllegalArgumentException unknownValueExc)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_TRANSPORT_TYPE,
                    "The given transport type '" + transportTypeStr + "' is invalid."
                ), unknownValueExc);
            }
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

            rscDfn = resourceDefinitionFactory.create(
                peerAccCtx.get(),
                rscName,
                extName,
                portInt,
                null, // RscDfnFlags
                secret,
                transportType,
                layerStack,
                peerSlotsRef,
                rscGrp
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            ApiCallRcImpl.EntryBuilder rcEntry = ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_INVLD_RSC_PORT,
                "The creation of a new resource definition failed due to an invalid TCP port number"
            );
            rcEntry.setCause(String.format("The specified number %d is not a valid TCP port number", portInt));
            rcEntry.setDetails(getCrtRscDfnName(rscName, generatedRscName));
            throw new ApiRcException(rcEntry.build(), exc);
        }
        catch (ExhaustedPoolException exc)
        {
            ApiCallRcImpl.EntryBuilder rcEntry = ApiCallRcImpl.entryBuilder(
                ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT,
                "The creation of a new resource definition failed, because no TCP port number\n" +
                "could be allocated for the resource definition"
            );
            rcEntry.setCause("TCP port number allocation failed, because the pool of free numbers is exhausted");
            rcEntry.setCorrection(
                "- Increase the size of the free TCP port number pool by extending the\n" +
                "  port number range for automatic allocation\n" +
                "or\n" +
                "- Delete existing resource definitions that have a TCP port number within\n" +
                "  the port number range for automatic allocation, if those resource definitions\n" +
                "  are no longer needed"
            );
            rcEntry.setDetails(getCrtRscDfnName(rscName, generatedRscName));
            throw new ApiRcException(rcEntry.build(), exc);
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
                "The creation of a new resource definition failed due to a name collision"
            );
            rcEntry.setCause("A resource definition with the name '" + rscNameStr + "' already exists");
            throw new ApiRcException(rcEntry.build(), exc);
        }
        return rscDfn;
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
}
