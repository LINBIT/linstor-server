package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataControllerFactory;
import com.linbit.linstor.ResourceDefinitionRepository;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotDefinition;
import com.linbit.linstor.SnapshotVolumeDefinition;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.VolumeDefinition.VlmDfnApi;
import com.linbit.linstor.VolumeDefinition.VlmDfnWtihCreationPayload;
import com.linbit.linstor.VolumeDefinitionData;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.VolumeNumberAlloc;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.apicallhandler.controller.helpers.ApiUtils;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.UuidUtils;

import static com.linbit.linstor.api.ApiConsts.API_UUID_NAME_PREFIX;
import static com.linbit.utils.StringUtils.firstLetterCaps;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

@Singleton
public class CtrlRscDfnApiCallHandler
{
    private final ErrorReporter errorReporter;
    private final AccessContext apiCtx;
    private final CtrlVlmDfnApiCallHandler vlmDfnHandler;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResourceDefinitionDataControllerFactory resourceDefinitionDataFactory;
    private final ResourceDefinitionRepository resourceDefinitionRepository;
    private final CtrlSatelliteUpdater ctrlSatelliteUpdater;
    private final ResponseConverter responseConverter;
    private final CtrlSecurityObjects secObjs;
    private final Provider<Peer> peer;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlRscDfnApiCallHandler(
        ErrorReporter errorReporterRef,
        @ApiContext AccessContext apiCtxRef,
        CtrlVlmDfnApiCallHandler vlmDfnHandlerRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResourceDefinitionDataControllerFactory resourceDefinitionDataFactoryRef,
        ResourceDefinitionRepository resourceDefinitionRepositoryRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef,
        ResponseConverter responseConverterRef,
        CtrlSecurityObjects secObjsRef,
        Provider<Peer> peerRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
    )
    {
        errorReporter = errorReporterRef;
        apiCtx = apiCtxRef;
        vlmDfnHandler = vlmDfnHandlerRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        resourceDefinitionDataFactory = resourceDefinitionDataFactoryRef;
        resourceDefinitionRepository = resourceDefinitionRepositoryRef;
        ctrlSatelliteUpdater = ctrlSatelliteUpdaterRef;
        responseConverter = responseConverterRef;
        secObjs = secObjsRef;
        peer = peerRef;
        peerAccCtx = peerAccCtxRef;
    }

    public ApiCallRc createResourceDefinition(
        String rscNameStr,
        byte[] extName,
        Integer portInt,
        String secret,
        String transportTypeStr,
        Map<String, String> props,
        List<VlmDfnWtihCreationPayload> volDescrMap,
        List<String> layerStackStrList
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeCreateOperation(),
            rscNameStr
        );

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

            ResourceDefinitionData rscDfn = createRscDfn(
                rscNameStr,
                extName,
                transportTypeStr,
                portInt,
                secret,
                layerStack
            );

            ctrlPropsHelper.fillProperties(LinStorObject.RESOURCE_DEFINITION, props,
                ctrlPropsHelper.getProps(rscDfn), ApiConsts.FAIL_ACC_DENIED_RSC_DFN);

            List<VolumeDefinitionData> createdVlmDfns = vlmDfnHandler.createVlmDfns(rscDfn, volDescrMap);

            resourceDefinitionRepository.put(apiCtx, rscDfn);

            ctrlTransactionHelper.commit();

            for (VolumeDefinitionData vlmDfn : createdVlmDfns)
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
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    private void warnIfMasterKeyIsNotSet(ApiCallRcImpl responsesRef)
    {
        byte[] masterKey = secObjs.getCryptKey();
        if ((masterKey == null || masterKey.length == 0))
        {
            String warnMsg = "The master key has not yet been set. Creating volume definitions within \n" +
                "an encrypted resource definitionto will fail!";

            errorReporter.logWarning(warnMsg);

            responsesRef.addEntry(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.WARN_NOT_FOUND_CRYPT_KEY,
                    warnMsg
                )
                .setCorrection("Create or enter the master passphrase, or remove the luks layer from the stack")
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

    public ApiCallRc modifyRscDfn(
        UUID rscDfnUuid,
        String rscNameStr,
        Integer portInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletePropNamespacesRef,
        List<String> layerStackStrListRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceDefinitionContext(
            ApiOperation.makeModifyOperation(),
            rscNameStr
        );

        try
        {
            requireRscDfnMapChangeAccess();

            ResourceName rscName = LinstorParsingUtils.asRscName(rscNameStr);
            ResourceDefinitionData rscDfn = ctrlApiDataLoader.loadRscDfn(rscName, true);
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
            if (portInt != null)
            {
                DrbdRscDfnData drbdRscDfn = rscDfn.getLayerData(apiCtx, DeviceLayerKind.DRBD);
                if (drbdRscDfn != null)
                {
                    drbdRscDfn.setPort(portInt);
                }
            }
            if (!overrideProps.isEmpty() || !deletePropKeys.isEmpty())
            {
                Props rscDfnProps = ctrlPropsHelper.getProps(rscDfn);

                ctrlPropsHelper.fillProperties(LinStorObject.RESOURCE_DEFINITION, overrideProps,
                    rscDfnProps, ApiConsts.FAIL_ACC_DENIED_RSC_DFN);
                ctrlPropsHelper.remove(rscDfnProps, deletePropKeys, deletePropNamespacesRef);
            }

            if (!layerStackStrListRef.isEmpty())
            {
                List<DeviceLayerKind> layerStack = LinstorParsingUtils.asDeviceLayerKind(layerStackStrListRef);

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

            responseConverter.addWithOp(responses, context, ApiSuccessUtils.defaultModifiedEntry(
                rscDfn.getUuid(), getRscDfnDescriptionInline(rscDfn)));
            responseConverter.addWithDetail(responses, context, ctrlSatelliteUpdater.updateSatellites(rscDfn));
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    ArrayList<ResourceDefinitionData.RscDfnApi> listResourceDefinitions()
    {
        ArrayList<ResourceDefinitionData.RscDfnApi> rscdfns = new ArrayList<>();
        try
        {
            for (ResourceDefinition rscdfn : resourceDefinitionRepository.getMapForView(peerAccCtx.get()).values())
            {
                try
                {
                    rscdfns.add(rscdfn.getApiData(peerAccCtx.get()));
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    // don't add storpooldfn without access
                }
            }
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

    private ResourceDefinitionData createRscDfn(
        String rscNameStr,
        byte[] extName,
        String transportTypeStr,
        Integer portInt,
        String secret,
        List<DeviceLayerKind> layerStack
    )
        throws InvalidNameException
    {
        TransportType transportType;
        if (transportTypeStr == null || transportTypeStr.trim().equals(""))
        {
            transportType = TransportType.IP;
        }
        else
        {
            try
            {
                transportType = TransportType.byValue(transportTypeStr); // TODO needs exception handling
            }
            catch (IllegalArgumentException unknownValueExc)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_TRANSPORT_TYPE,
                    "The given transport type '" + transportTypeStr + "' is invalid."
                ), unknownValueExc);
            }
        }

        if (rscNameStr == null)
        {
            throw new ImplementationError("Resource name must not be null!");
        }

        ResourceName rscName;
        if (!rscNameStr.equals(""))
        {
            if (extName != null && extName.length != 0)
            {
                throw new ApiException("Either a resource name or an external name must be present, but not both!");
            }
            else
            {
                rscName = LinstorParsingUtils.asRscName(rscNameStr);
            }
        }
        else
        {
            if (extName == null)
            {
                throw new ApiException("Either a resource name or an external name must be present!");
            }
            else if (extName.length == 0)
            {
                String uuidStr = API_UUID_NAME_PREFIX + UuidUtils.asUuid(extName).toString().toUpperCase();
                rscName = new ResourceName(uuidStr);
            }
            else
            {
                rscName = ApiUtils.createResourceName(extName);
                try
                {
                    if (resourceDefinitionRepository.getMapForViewExtName(peerAccCtx.get()).containsKey(extName))
                    {
                        throw new ApiException("External name already taken!");
                    }
                    if (resourceDefinitionRepository.getMapForView(peerAccCtx.get()).containsKey(rscName))
                    {
                        String uuidStr = API_UUID_NAME_PREFIX + UUID.randomUUID().toString().toUpperCase();
                        rscName = new ResourceName(uuidStr);
                    }
                }
                catch (AccessDeniedException accDeniedExc)
                {
                    throw new ApiAccessDeniedException(
                        accDeniedExc,
                        "getMapForView " + getRscDfnDescriptionInline(rscName.toString()),
                        ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                    );
                }
            }
        }

        if (!layerStack.isEmpty())
        {
            CtrlRscCrtApiHelper.ensureLayerStackIsAllowed(layerStack);
        }

        ResourceDefinitionData rscDfn;
        try
        {
            rscDfn = resourceDefinitionDataFactory.create(
                peerAccCtx.get(),
                rscName,
                extName,
                portInt,
                null, // RscDfnFlags
                secret,
                transportType,
                layerStack
            );
        }
        catch (ValueOutOfRangeException | ValueInUseException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(ApiConsts.FAIL_INVLD_RSC_PORT, String.format(
                "The specified TCP port '%d' is invalid.",
                portInt
            )), exc);
        }
        catch (ExhaustedPoolException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_POOL_EXHAUSTED_TCP_PORT,
                "Could not find free TCP port"
            ), exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getRscDfnDescriptionInline(rscNameStr),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (SQLException sqlExc)
        {
            throw new ApiSQLException(sqlExc);
        }
        catch (LinStorDataAlreadyExistsException exc)
        {
            throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                ApiConsts.FAIL_EXISTS_RSC_DFN,
                firstLetterCaps(getRscDfnDescriptionInline(rscNameStr)) + " already exists."
            ), exc);
        }
        return rscDfn;
    }

    static VolumeNumber getVlmNr(VlmDfnApi vlmDfnApi, ResourceDefinition rscDfn, AccessContext accCtx)
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
}
