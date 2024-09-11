package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.utils.LinstorIteratorUtils;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.repository.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.utils.ExceptionThrowingSupplier;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

@Singleton
public class PropsChangedListenerBuilder
{
    private final Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerProvider;
    private final SystemConfRepository systemConfRepository;
    private final ResourceDefinitionProtectionRepository rscDfnProtRepo;
    private final CtrlSatelliteUpdateCaller satelliteUpdateCaller;
    private final ErrorReporter errorReporter;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;

    @Inject
    public PropsChangedListenerBuilder(
        Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerProviderRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionProtectionRepository rscDfnProtRepoRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ErrorReporter errorReporterRef,
        CtrlRscLayerDataFactory ctrlRscLayerDataFactoryRef
    )
    {
        ctrlRscDfnApiCallHandlerProvider = ctrlRscDfnApiCallHandlerProviderRef;
        systemConfRepository = systemConfRepositoryRef;
        rscDfnProtRepo = rscDfnProtRepoRef;
        satelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        errorReporter = errorReporterRef;
        ctrlRscLayerDataFactory = ctrlRscLayerDataFactoryRef;
    }

    // controller
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> systemConfRepository.getCtrlConfForChange(accCtx));
        builder.setRscDfnListSupplier(() -> rscDfnProtRepo.getMapForView(accCtx).values());
        builder.setRscListSupplier(builder.buildRscListSupplierFromRscDfnSupplier(accCtx));

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addSkipDisk();
        return builder.propsChangedListeners;
    }

    // rscGrp
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        ResourceGroup rscGrp,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> rscGrp.getProps(accCtx));
        builder.setRscDfnListSupplier(() -> LinstorIteratorUtils.getRscDfns(accCtx, rscGrp));
        builder.setRscListSupplier(builder.buildRscListSupplierFromRscDfnSupplier(accCtx));

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addSkipDisk();
        return builder.propsChangedListeners;
    }

    // vlmGrp
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        VolumeGroup vlmGrp,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> vlmGrp.getProps(accCtx));
        builder.setRscDfnListSupplier(() -> LinstorIteratorUtils.getRscDfns(accCtx, vlmGrp));
        builder.setRscListSupplier(builder.buildRscListSupplierFromRscDfnSupplier(accCtx));

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        return builder.propsChangedListeners;
    }

    // rscDfn
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> rscDfn.getProps(accCtx));
        builder.setRscDfnListSupplier(() -> Collections.singletonList(rscDfn));
        builder.setRscListSupplier(builder.buildRscListSupplierFromRscDfnSupplier(accCtx));

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addSkipDisk();
        return builder.propsChangedListeners;
    }

    // vlmDfn
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        VolumeDefinition vlmDfn,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> vlmDfn.getProps(accCtx));
        builder.setRscDfnListSupplier(() -> Collections.singletonList(vlmDfn.getResourceDefinition()));
        builder.setRscListSupplier(builder.buildRscListSupplierFromRscDfnSupplier(accCtx));

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        return builder.propsChangedListeners;
    }

    // node
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        Node nodeRef,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> nodeRef.getProps(accCtx));
        // rscDfnListSupplier is (currently) not needed
        // builder.setRscDfnListSupplier(() -> getRscDfnsByStorPool(accCtx, storPoolRef));
        builder.setRscListSupplier(() -> getRscsBy(accCtx, nodeRef));

        builder.addSkipDisk();
        return builder.propsChangedListeners;
    }

    private Collection<Resource> getRscsBy(AccessContext accCtxRef, Node nodeRef)
        throws AccessDeniedException
    {
        Collection<Resource> ret = new LinkedHashSet<>();
        Iterator<Resource> rscIt = nodeRef.iterateResources(accCtxRef);
        while (rscIt.hasNext())
        {
            ret.add(rscIt.next());
        }
        return ret;
    }

    // storPool
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        StorPool storPoolRef,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> storPoolRef.getProps(accCtx));
        // rscDfnListSupplier is (currently) not needed
        // builder.setRscDfnListSupplier(() -> getRscDfnsByStorPool(accCtx, storPoolRef));
        builder.setRscListSupplier(() -> getRscsBy(accCtx, storPoolRef));

        builder.addSkipDisk();
        return builder.propsChangedListeners;
    }

    private Collection<Resource> getRscsBy(AccessContext accCtxRef, StorPool storPoolRef)
        throws AccessDeniedException
    {
        Collection<Resource> ret = new LinkedHashSet<>();
        for (VlmProviderObject<Resource> vlmData : storPoolRef.getVolumes(accCtxRef))
        {
            ret.add(vlmData.getRscLayerObject().getAbsResource());
        }
        return ret;
    }

    // rsc
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        AccessContext accCtx,
        Resource rscRef,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> rscRef.getProps(accCtx));
        builder.setRscDfnListSupplier(() -> Collections.singletonList(rscRef.getResourceDefinition()));
        builder.setRscListSupplier(() -> Collections.singleton(rscRef));

        builder.addSkipDisk();
        return builder.propsChangedListeners;
    }

    private class Builder
    {
        private final List<Flux<ApiCallRc>> fluxes;
        private final Map<String, PropertyChangedListener> propsChangedListeners = new HashMap<>();

        private @Nullable ExceptionThrowingSupplier<Props, AccessDeniedException> currentPropSupplier;
        private @Nullable ExceptionThrowingSupplier<Collection<ResourceDefinition>, AccessDeniedException> rscDfnsSupplier;
        private @Nullable ExceptionThrowingSupplier<Collection<Resource>, AccessDeniedException> rscsSupplier;

        Builder(List<Flux<ApiCallRc>> fluxesRef)
        {
            fluxes = fluxesRef;
        }

        void setCurrentPropSupplier(ExceptionThrowingSupplier<Props, AccessDeniedException> currentPropSupplierRef)
        {
            currentPropSupplier = currentPropSupplierRef;
        }

        void setRscDfnListSupplier(
            ExceptionThrowingSupplier<Collection<ResourceDefinition>, AccessDeniedException> rscDfnsSupplierRef
        )
        {
            rscDfnsSupplier = rscDfnsSupplierRef;
        }

        void setRscListSupplier(
            ExceptionThrowingSupplier<Collection<Resource>, AccessDeniedException> rscsSupplierRef
        )
        {
            rscsSupplier = rscsSupplierRef;
        }

        ExceptionThrowingSupplier<Collection<Resource>, AccessDeniedException> buildRscListSupplierFromRscDfnSupplier(
            AccessContext accCtxRef
        )
        {
            return () -> {
                List<Resource> ret = new ArrayList<>();
                for (ResourceDefinition rscDfn : rscDfnsSupplier.supply())
                {
                    Iterator<Resource> iterateResource = rscDfn.iterateResource(accCtxRef);
                    while (iterateResource.hasNext())
                    {
                        ret.add(iterateResource.next());
                    }
                }
                return ret;
            };
        }

        void addDrbdOptionsDiskRsDiscardGranularity()
        {
            CtrlRscDfnApiCallHandler ctrlRscDfnHandler = ctrlRscDfnApiCallHandlerProvider.get();
            require(currentPropSupplier, "current property supplier");
            require(rscDfnsSupplier, "rscDfns supplier");
            propsChangedListeners.put(
                CtrlRscDfnApiCallHelper.FULL_KEY_DISC_GRAN,
                (ignoredKey, newVal, oldValue) ->
                {
                    if (!Objects.equals(newVal, oldValue))
                    {
                        try
                        {
                            if (newVal != null)
                            {
                                // disable the auto-prop on the current level
                                currentPropSupplier.supply()
                                    .setProp(
                                        ApiConsts.NAMESPC_DRBD_OPTIONS + "/" +
                                            ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                                        ApiConsts.VAL_FALSE
                                    );
                            }
                            for (ResourceDefinition rscDfn : rscDfnsSupplier.supply())
                            {
                                fluxes.add(ctrlRscDfnHandler.updateProps(rscDfn));
                            }
                        }
                        catch (InvalidKeyException | InvalidValueException exc)
                        {
                            throw new ImplementationError(exc);
                        }
                    }
                }
            );
            addDrbdOptionsAutoRsDiscardGranularity();
        }

        void addDrbdOptionsAutoRsDiscardGranularity()
        {
            require(currentPropSupplier, "current property supplier");
            require(rscDfnsSupplier, "rscDfns supplier");
            CtrlRscDfnApiCallHandler ctrlRscDfnHandler = ctrlRscDfnApiCallHandlerProvider.get();
            propsChangedListeners.put(
                ApiConsts.NAMESPC_DRBD_OPTIONS + "/" +
                    ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                (ignoredKey, newVal, oldValue) ->
                {
                    if (!Objects.equals(newVal, oldValue))
                    {
                        for (ResourceDefinition rscDfn : rscDfnsSupplier.supply())
                        {
                            fluxes.add(ctrlRscDfnHandler.updateProps(rscDfn));
                        }
                    }
                }
            );
        }

        void addSkipDisk()
        {
            addRecalculateVolatileDataProperty(ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_SKIP_DISK);
        }

        void addRecalculateVolatileDataProperty(String propRef)
        {
            require(currentPropSupplier, "current property supplier");
            require(rscsSupplier, "resources supplier");
            propsChangedListeners.put(
                propRef,
                (ignoredKey, newValue, oldValue) ->
                {
                    if (!Objects.equals(newValue, oldValue))
                    {
                        Set<ResourceDefinition> rscDfnToUpdate = new TreeSet<>();
                        for (Resource rsc : rscsSupplier.supply())
                        {
                            ResourceDataUtils.recalculateVolatileRscData(ctrlRscLayerDataFactory, rsc);
                            rscDfnToUpdate.add(rsc.getResourceDefinition());
                        }
                        for (ResourceDefinition rscDfn : rscDfnToUpdate)
                        {
                            fluxes.add(
                                satelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
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
            );
        }

        private void require(Object requiredSupplier, String supplierDescrRef)
        {
            if (requiredSupplier == null)
            {
                throw new ImplementationError("Missing " + supplierDescrRef);
            }
        }
    }
}
