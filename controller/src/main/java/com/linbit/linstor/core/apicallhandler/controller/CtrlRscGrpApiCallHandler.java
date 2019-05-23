package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.LinstorParsingUtils;
import com.linbit.linstor.VolumeNumberAlloc;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.pojo.RscGrpPojo;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdater;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AutoSelectorConfigData;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.ResourceGroupData;
import com.linbit.linstor.core.objects.ResourceGroupDataControllerFactory;
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

import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.google.inject.Provider;

@Singleton
public class CtrlRscGrpApiCallHandler
{
    private final ErrorReporter errorReporter;
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

    public CtrlRscGrpApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        Provider<Peer> peerRef,
        Provider<AccessContext> peerAccCtxRef,
        ResourceGroupRepository rscGrpRepoRef,
        ResourceGroupDataControllerFactory rscGrpFactoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        CtrlVlmGrpApiCallHandler ctrlVlmGrpApiCallHandlerRef,
        CtrlSatelliteUpdater ctrlSatelliteUpdaterRef
    )
    {
        errorReporter = errorReporterRef;
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

    public ApiCallRc modify(
        String rscGrpNameStrRef,
        String descriptionRef,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeysRef,
        HashSet<String> deleteNamespacesRef,
        List<String> overrideLayerStackListRef,
        AutoSelectFilterApi autoApiRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();
        ResponseContext context = makeResourceGroupContext(
            ApiOperation.makeCreateOperation(),
            rscGrpNameStrRef
        );

        try
        {
            requireRscGrpMapChangeAccess();

            ResourceGroupData rscGrpData = ctrlApiDataLoader.loadResourceGroup(rscGrpNameStrRef, true);

            if (descriptionRef != null)
            {
                rscGrpData.setDescription(peerAccCtx.get(), descriptionRef);
            }

            if (!overrideProps.isEmpty() || !deletePropKeysRef.isEmpty() || !deleteNamespacesRef.isEmpty())
            {
                Props rscDfnGrpProps = rscGrpData.getRscDfnGrpProps(peerAccCtx.get());
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

            if (!overrideLayerStackListRef.isEmpty())
            {
                List<DeviceLayerKind> layerStack = new ArrayList<>();
                for (String devLayerKindStr : overrideLayerStackListRef)
                {
                    layerStack.add(DeviceLayerKind.valueOf(devLayerKindStr.toUpperCase()));
                }
                rscGrpData.setLayerStack(peerAccCtx.get(), layerStack);
            }

            if (autoApiRef != null)
            {
                AutoSelectorConfigData autoPlaceConfig =
                    (AutoSelectorConfigData) rscGrpData.getAutoPlaceConfig();
                autoPlaceConfig.applyChanges(autoApiRef);
            }

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultModifiedEntry(
                    rscGrpData.getUuid(), getRscGrpDescriptionInline(rscGrpData)
                )
            );

            for (ResourceDefinition rscDfn : rscGrpData.getRscDfns(peerAccCtx.get()))
            {
                responseConverter.addWithDetail(
                    responses,
                    context,
                    ctrlSatelliteUpdater.updateSatellites(rscDfn)
                );
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
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
            String rscGrpDisplayValue = rscGrpData.getName().displayValue;
            deleteResourceGroup(rscGrpData);

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
            rscGrp = rscGrpFactory.create(
                peerAccCtx.get(),
                LinstorParsingUtils.asRscGrpName(rscGrpPojoRef.getName()),
                rscGrpPojoRef.getDescription(),
                rscGrpPojoRef.getLayerStack(),
                autoSelectFilter.getReplicaCount(),
                autoSelectFilter.getStorPoolNameStr(),
                autoSelectFilter.getDoNotPlaceWithRscList(),
                autoSelectFilter.getDoNotPlaceWithRscRegex(),
                autoSelectFilter.getReplicasOnSameList(),
                autoSelectFilter.getReplicasOnDifferentList(),
                autoSelectFilter.getProviderList()
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


    static VolumeNumber getVlmNr(VlmGrpApi vlmGrpApi, ResourceGroup rscGrp, AccessContext accCtx)
        throws LinStorException, ValueOutOfRangeException
    {
        VolumeNumber vlmNr;
        Integer vlmNrRaw = vlmGrpApi.getVolumeNr();
        if (vlmNrRaw == null)
        {
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
        }
        else
        {
            vlmNr = new VolumeNumber(vlmNrRaw);
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
