package com.linbit.linstor.core.apicallhandler.controller;


import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.LinStorDataAlreadyExistsException;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRc.RcEntry;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.helpers.PropsChangedListenerBuilder;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSuccessUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.apis.VolumeGroupApi;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.objects.VolumeGroup.Flags;
import com.linbit.linstor.core.objects.VolumeGroupControllerFactory;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.locks.LockGuardFactory;
import com.linbit.utils.PairNonNull;

import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscGrpApiCallHandler.getRscGrpDescriptionInline;
import static com.linbit.locks.LockGuardFactory.LockObj.NODES_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.RSC_GRP_MAP;
import static com.linbit.locks.LockGuardFactory.LockObj.STOR_POOL_DFN_MAP;
import static com.linbit.locks.LockGuardFactory.LockType.WRITE;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlVlmGrpApiCallHandler
{
    private final AccessContext apiCtx;
    private final ErrorReporter errorReporter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final Provider<Peer> peer;
    private final VolumeGroupControllerFactory volumeGroupFactory;
    private final CtrlPropsHelper ctrlPropsHelper;
    private final CtrlApiDataLoader ctrlApiDataLoader;
    private final ResponseConverter responseConverter;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;
    private final Provider<PropsChangedListenerBuilder> propsChangeListenerBuilder;

    @Inject
    public CtrlVlmGrpApiCallHandler(
        @ApiContext AccessContext apiCtxRef,
        ErrorReporter errorReporterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        Provider<Peer> peerRef,
        VolumeGroupControllerFactory volumeGroupControllerFactoryRef,
        CtrlPropsHelper ctrlPropsHelperRef,
        CtrlApiDataLoader ctrlApiDataLoaderRef,
        ResponseConverter responseConverterRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        Provider<PropsChangedListenerBuilder> propsChangeListenerBuilderRef
    )
    {
        apiCtx = apiCtxRef;
        errorReporter = errorReporterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        peerAccCtx = peerAccCtxRef;
        peer = peerRef;
        volumeGroupFactory = volumeGroupControllerFactoryRef;
        ctrlPropsHelper = ctrlPropsHelperRef;
        ctrlApiDataLoader = ctrlApiDataLoaderRef;
        responseConverter = responseConverterRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        propsChangeListenerBuilder = propsChangeListenerBuilderRef;
    }

    public <T extends VolumeGroupApi> List<VolumeGroup> createVlmGrps(
        ApiCallRcImpl responses,
        ResourceGroup rscGrpRef,
        List<T> vlmGrpPojoListRef
    )
        throws AccessDeniedException
    {
        if (!rscGrpRef.getRscDfns(peerAccCtx.get()).isEmpty())
        {
            throw new ApiRcException(
                ApiCallRcImpl.entryBuilder(
                    ApiConsts.FAIL_EXISTS_RSC_DFN,
                    "Volume group cannot be created while the resource group has already resource definitions."
                )
                .build()
            );
        }

        List<VolumeGroup> vlmGrps = new ArrayList<>();
        for (VolumeGroupApi vlmGrpPojo : vlmGrpPojoListRef)
        {
            vlmGrps.add(createVolumeGroup(responses, rscGrpRef, vlmGrpPojo));
        }
        return vlmGrps;
    }

    public <T extends VolumeGroupApi> ApiCallRc createVlmGrps(
        String rscGrpNameRef,
        List<T> vlmGrpApiListRef
    )
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_VLM_GRP, rscGrpNameRef);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeCreateOperation(),
            "Volume groups for " + getRscGrpDescriptionInline(rscGrpNameRef),
            "volume groups for " + getRscGrpDescriptionInline(rscGrpNameRef),
            ApiConsts.MASK_VLM_GRP,
            objRefs
        );

        try
        {
            if (vlmGrpApiListRef.isEmpty())
            {
                throw new ApiRcException(
                    ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_WARN,
                        "Volume group list to create is empty."
                    )
                    .setDetails("Volume group list that should be added to the resource group is empty.")
                    .build()
                );
            }

            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameRef, true);

            List<VolumeGroup> vlmGrpsCreated = createVlmGrps(responses, rscGrp, vlmGrpApiListRef);

            ctrlTransactionHelper.commit();

            for (VolumeGroup vlmGrp : vlmGrpsCreated)
            {
                responseConverter.addWithOp(responses, context, createVlmGrpCrtSuccessEntry(vlmGrp, rscGrpNameRef));
            }
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }

        return responses;
    }

    public List<VolumeGroupApi> listVolumeGroups(String rscGrpNameRef, @Nullable Integer vlmNrRef)
    {
        List<VolumeGroupApi> ret;
        try
        {
            if (vlmNrRef != null)
            {
                VolumeGroup vlmGrp = ctrlApiDataLoader.loadVlmGrp(rscGrpNameRef, vlmNrRef, true);
                if (vlmGrp == null)
                {
                    ret = Collections.emptyList();
                }
                else
                {
                    ret = Arrays.asList(vlmGrp.getApiData(peerAccCtx.get()));
                }
            }
            else
            {
                ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameRef, true);
                ret = rscGrp.getApiData(peerAccCtx.get()).getVlmGrpList();
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "list " + getVlmGrpDescriptionInline(rscGrpNameRef, vlmNrRef),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP
            );
        }
        return ret;
    }

    public Flux<ApiCallRc> modify(
        String rscGrpNameStr,
        int vlmNrInt,
        Map<String, String> overrideProps,
        HashSet<String> deletePropKeys,
        HashSet<String> deleteNamespaces,
        List<String> flagsList
    )
    {
        ResponseContext context = makeVolumeGroupContext(
            ApiOperation.makeModifyOperation(),
            rscGrpNameStr,
            vlmNrInt
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Modify volume group",
                lockGuardFactory.buildDeferred(
                    WRITE,
                    NODES_MAP, RSC_DFN_MAP, STOR_POOL_DFN_MAP, RSC_GRP_MAP
                ),
                () -> modifyVlmGrpInTransaction(
                    rscGrpNameStr,
                    vlmNrInt,
                    overrideProps,
                    deletePropKeys,
                    deleteNamespaces,
                    flagsList
                )
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> modifyVlmGrpInTransaction(
        String rscGrpNameStr,
        int vlmNrInt,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deleteNamespaces,
        List<String> flagsListRef
    )
    {
        List<Flux<Flux<ApiCallRc>>> fluxes = new ArrayList<>();
        List<Flux<ApiCallRc>> specialPropFluxes = new ArrayList<>();
        ApiCallRcImpl apiCallRcs = new ApiCallRcImpl();
        boolean notifyStlts;

        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_VLM_GRP, rscGrpNameStr);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeCreateOperation(),
            "Volume groups for " + getRscGrpDescriptionInline(rscGrpNameStr),
            "volume groups for " + getRscGrpDescriptionInline(rscGrpNameStr),
            ApiConsts.MASK_VLM_GRP,
            objRefs
        );
        try
        {
            List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
            prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            VolumeGroup vlmGrp = ctrlApiDataLoader.loadVlmGrp(rscGrpNameStr, vlmNrInt, true);

            Map<String, PropertyChangedListener> propsChangedListeners = propsChangeListenerBuilder.get()
                .buildPropsChangedListeners(peerAccCtx.get(), vlmGrp, specialPropFluxes);

            Props props = vlmGrp.getProps(peerAccCtx.get());
            notifyStlts = ctrlPropsHelper.fillProperties(
                apiCallRcs,
                LinStorObject.VLM_DFN,
                overrideProps,
                props,
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP,
                prefixesIgnoringWhitelistCheck,
                propsChangedListeners
            );
            notifyStlts = ctrlPropsHelper.remove(
                apiCallRcs,
                LinStorObject.VLM_DFN,
                props,
                deletePropKeys,
                deleteNamespaces,
                prefixesIgnoringWhitelistCheck,
                propsChangedListeners
            ) || notifyStlts;

            PairNonNull<Set<VolumeGroup.Flags>, Set<VolumeGroup.Flags>> pair = FlagsHelper
                .extractFlagsToEnableOrDisable(
                    VolumeGroup.Flags.class,
                    flagsListRef
                );

            StateFlags<Flags> vlmGrpFlags = vlmGrp.getFlags();
            for (VolumeGroup.Flags flag : pair.objA)
            {
                notifyStlts = true;
                vlmGrpFlags.enableFlags(apiCtx, flag);
            }
            for (VolumeGroup.Flags flag : pair.objB)
            {
                notifyStlts = true;
                vlmGrpFlags.disableFlags(apiCtx, flag);
            }

            ctrlTransactionHelper.commit();

            ResourceGroup rscGrp = ctrlApiDataLoader.loadResourceGroup(rscGrpNameStr, true);

            responseConverter.addWithOp(
                apiCallRcs,
                context,
                ApiSuccessUtils.defaultModifiedEntry(
                    rscGrp.getUuid(), getRscGrpDescriptionInline(rscGrp)
                )
            );

            if (notifyStlts)
            {
                for (ResourceDefinition rscDfn : rscGrp.getRscDfns(peerAccCtx.get()))
                {
                    fluxes.add(
                        Flux.just(
                            ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                                .flatMap(updateTuple -> updateTuple == null ? Flux.empty() : updateTuple.getT2())
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "modify " + getVlmGrpDescriptionInline(rscGrpNameStr, vlmNrInt),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP
            );
        }
        catch (Exception | ImplementationError exc)
        {
            apiCallRcs = responseConverter.reportException(peer.get(), context, exc);
        }

        return Flux.just((ApiCallRc) apiCallRcs)
            .concatWith(CtrlResponseUtils.mergeExtractingApiRcExceptions(errorReporter, Flux.merge(fluxes)))
            .concatWith(Flux.merge(specialPropFluxes));
    }

    public ApiCallRc delete(String rscGrpNameRef, int vlmNrRef)
    {
        ApiCallRcImpl responses = new ApiCallRcImpl();

        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_VLM_GRP, rscGrpNameRef);

        ResponseContext context = new ResponseContext(
            ApiOperation.makeCreateOperation(),
            "Volume groups for " + getRscGrpDescriptionInline(rscGrpNameRef),
            "volume groups for " + getRscGrpDescriptionInline(rscGrpNameRef),
            ApiConsts.MASK_VLM_GRP,
            objRefs
        );
        try
        {
            VolumeGroup vlmGrp = ctrlApiDataLoader.loadVlmGrp(rscGrpNameRef, vlmNrRef, true);
            final UUID vlmGrpUUID = vlmGrp.getUuid();
            int vlmNr = vlmGrp.getVolumeNumber().value;
            vlmGrp.delete(peerAccCtx.get());

            ctrlTransactionHelper.commit();

            responseConverter.addWithOp(
                responses,
                context,
                ApiSuccessUtils.defaultDeletedEntry(
                    vlmGrpUUID, getVlmGrpDescriptionInline(rscGrpNameRef, vlmNr)
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "delete " + getVlmGrpDescriptionInline(rscGrpNameRef, vlmNrRef),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP
            );
        }
        catch (Exception | ImplementationError exc)
        {
            responses = responseConverter.reportException(peer.get(), context, exc);
        }
        return responses;

    }

    private VolumeGroup createVolumeGroup(
        ApiCallRcImpl responses,
        ResourceGroup rscGrpRef,
        VolumeGroupApi vlmGrpApiRef
    )
    {
        VolumeNumber vlmNr = getOrGenerateVlmNr(vlmGrpApiRef, rscGrpRef);

        ResponseContext context = makeVolumeGroupContext(
            ApiOperation.makeCreateOperation(),
            rscGrpRef.getName().displayValue,
            vlmNr.value
        );

        VolumeGroup vlmGrp;

        try
        {
            vlmGrp = createVolumeGroup(
                peerAccCtx.get(),
                rscGrpRef,
                vlmNr,
                vlmGrpApiRef.getFlags()
            );

            List<String> prefixesIgnoringWhitelistCheck = new ArrayList<>();
            prefixesIgnoringWhitelistCheck.add(ApiConsts.NAMESPC_EBS + "/" + ApiConsts.NAMESPC_TAGS + "/");

            ctrlPropsHelper.fillProperties(
                responses,
                LinStorObject.VLM_DFN,
                vlmGrpApiRef.getProps(),
                getVlmGrpProps(vlmGrp),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP,
                prefixesIgnoringWhitelistCheck
            );
        }
        catch (Exception | ImplementationError exc)
        {
            throw new ApiRcException(
                responseConverter.exceptionToResponse(
                    peer.get(),
                    context,
                    exc
                ),
                exc,
                true
            );
        }
        return vlmGrp;
    }

    private VolumeGroup createVolumeGroup(
        AccessContext accCtx,
        ResourceGroup rscGrp,
        VolumeNumber vlmNr,
        long initFlags
    )
    {
        VolumeGroup vlmGrp;
        try
        {
            vlmGrp = volumeGroupFactory.create(
                accCtx,
                rscGrp,
                vlmNr,
                initFlags
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "create " + getVlmGrpDescriptionInline(rscGrp, vlmNr),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP
            );
        }
        catch (LinStorDataAlreadyExistsException dataAlreadyExistsExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_EXISTS_VLM_GRP,
                    String.format(
                        "A volume group with the number %d already exists in resource group '%s'.",
                        vlmNr.value,
                        rscGrp.getName().getDisplayName()
                    ),
                    true
                ),
                dataAlreadyExistsExc
            );
        }
        catch (DatabaseException dbExc)
        {
            throw new ApiDatabaseException(dbExc);
        }
        return vlmGrp;
    }

    private VolumeNumber getOrGenerateVlmNr(VolumeGroupApi vlmGrpApi, ResourceGroup rscGrp)
    {
        VolumeNumber vlmNr;
        try
        {
            vlmNr = CtrlRscGrpApiCallHandler.getVlmNr(vlmGrpApi, rscGrp, apiCtx);
        }
        catch (ValueOutOfRangeException valOORangeExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_INVLD_VLM_NR,
                    String.format(
                        "The specified volume number '%d' is invalid. Volume numbers have to be in range of %d - %d.",
                        vlmGrpApi.getVolumeNr(),
                        VolumeNumber.VOLUME_NR_MIN,
                        VolumeNumber.VOLUME_NR_MAX
                    )
                ),
                valOORangeExc
            );
        }
        catch (LinStorException linStorExc)
        {
            throw new ApiRcException(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.FAIL_POOL_EXHAUSTED_VLM_NR,
                    "An exception occurred during generation of a volume number."
                ),
                linStorExc
            );
        }
        return vlmNr;
    }

    private Props getVlmGrpProps(VolumeGroup vlmGrpRef)
    {
        Props props;
        try
        {
            props = vlmGrpRef.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access the properties of " + getVlmGrpDescriptionInline(vlmGrpRef),
                ApiConsts.FAIL_ACC_DENIED_VLM_GRP
            );
        }
        return props;
    }

    private RcEntry createVlmGrpCrtSuccessEntry(VolumeGroup vlmGrp, String rscGrpNameRef)
    {
        ApiCallRcEntry vlmGrpCrtSuccessEntry = new ApiCallRcEntry();
        vlmGrpCrtSuccessEntry.setReturnCode(ApiConsts.CREATED);
        String successMessage = String.format(
            "New volume group with number '%d' of resource group '%s' created.",
            vlmGrp.getVolumeNumber().value,
            rscGrpNameRef
        );
        vlmGrpCrtSuccessEntry.setMessage(successMessage);
        vlmGrpCrtSuccessEntry.putObjRef(ApiConsts.KEY_RSC_GRP, rscGrpNameRef);
        vlmGrpCrtSuccessEntry.putObjRef(ApiConsts.KEY_VLM_NR, Integer.toString(vlmGrp.getVolumeNumber().value));
        errorReporter.logInfo(successMessage);
        return vlmGrpCrtSuccessEntry;
    }

    public static String getVlmGrpDescription(String rscGrpName, Integer vlmNr)
    {
        return "Resource group: " + rscGrpName + ", Volume number: " + vlmNr;
    }

    public static String getVlmGrpDescriptionInline(VolumeGroup vlmGrpData)
    {
        return getVlmGrpDescriptionInline(vlmGrpData.getResourceGroup(), vlmGrpData.getVolumeNumber());
    }

    public static String getVlmGrpDescriptionInline(ResourceGroup rscGrp, VolumeNumber volNr)
    {
        return getVlmGrpDescriptionInline(rscGrp.getName().displayValue, volNr.value);
    }

    public static String getVlmGrpDescriptionInline(String rscName, @Nullable Integer vlmNr)
    {
        return "volume group with number '" + vlmNr + "' of resource group '" + rscName + "'";
    }

    static ResponseContext makeVolumeGroupContext(
        ApiOperation operation,
        String  rscGrpNameStr,
        int volumeNr
    )
    {
        Map<String, String> objRefs = new TreeMap<>();
        objRefs.put(ApiConsts.KEY_RSC_GRP, rscGrpNameStr);
        objRefs.put(ApiConsts.KEY_VLM_NR, Integer.toString(volumeNr));

        return new ResponseContext(
            operation,
            getVlmGrpDescription(rscGrpNameStr, volumeNr),
            getVlmGrpDescriptionInline(rscGrpNameStr, volumeNr),
            ApiConsts.MASK_VLM_DFN,
            objRefs
        );
    }
}
