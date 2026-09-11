package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
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
import com.linbit.linstor.core.repository.ResourceDefinitionRepositoryImpl;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

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
import java.util.function.Supplier;

import reactor.core.publisher.Flux;

@Singleton
public class PropsChangedListenerBuilder
{
    private final Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerProvider;
    private final SystemConfRepository systemConfRepository;
    private final ResourceDefinitionRepositoryImpl rscDfnProtRepo;
    private final CtrlSatelliteUpdateCaller satelliteUpdateCaller;
    private final ErrorReporter errorReporter;
    private final CtrlRscLayerDataFactory ctrlRscLayerDataFactory;

    @Inject
    public PropsChangedListenerBuilder(
        Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerProviderRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionRepositoryImpl rscDfnProtRepoRef,
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
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> systemConfRepository.getCtrlConfForChange());
        builder.setRscDfnListSupplier(() -> rscDfnProtRepo.getMapForView().values());
        builder.setRscListSupplier(builder::buildRscListFromRscDfnSupplier);

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addDrbdOptionsDiskDiscardGranularity();
        builder.addSkipDisk();
        builder.addLuksAllowDiscards();
        return builder.propsChangedListeners;
    }

    // rscGrp
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        ResourceGroup rscGrp,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> rscGrp.getProps());
        builder.setRscDfnListSupplier(() -> LinstorIteratorUtils.getRscDfns(rscGrp));
        builder.setRscListSupplier(builder::buildRscListFromRscDfnSupplier);

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addDrbdOptionsDiskDiscardGranularity();
        builder.addSkipDisk();
        builder.addLuksAllowDiscards();
        return builder.propsChangedListeners;
    }

    // vlmGrp
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        VolumeGroup vlmGrp,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> vlmGrp.getProps());
        builder.setRscDfnListSupplier(() -> LinstorIteratorUtils.getRscDfns(vlmGrp));
        builder.setRscListSupplier(builder::buildRscListFromRscDfnSupplier);

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addLuksAllowDiscards();
        builder.addDrbdOptionsDiskDiscardGranularity();
        return builder.propsChangedListeners;
    }

    // rscDfn
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        ResourceDefinition rscDfn,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> rscDfn.getProps());
        builder.setRscDfnListSupplier(() -> Collections.singletonList(rscDfn));
        builder.setRscListSupplier(builder::buildRscListFromRscDfnSupplier);

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addDrbdOptionsDiskDiscardGranularity();
        builder.addSkipDisk();
        builder.addLuksAllowDiscards();
        return builder.propsChangedListeners;
    }

    // vlmDfn
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        VolumeDefinition vlmDfn,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> vlmDfn.getProps());
        builder.setRscDfnListSupplier(() -> Collections.singletonList(vlmDfn.getResourceDefinition()));
        builder.setRscListSupplier(builder::buildRscListFromRscDfnSupplier);

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        builder.addLuksAllowDiscards();
        builder.addDrbdOptionsDiskDiscardGranularity();
        return builder.propsChangedListeners;
    }

    // node
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        Node nodeRef,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> nodeRef.getProps());
        // rscDfnListSupplier is (currently) not needed
        // builder.setRscDfnListSupplier(() -> getRscDfnsByStorPool(storPoolRef));
        builder.setRscListSupplier(() -> getRscsBy(nodeRef));

        builder.addSkipDisk();
        builder.addLuksAllowDiscards();
        return builder.propsChangedListeners;
    }

    private Collection<Resource> getRscsBy(Node nodeRef)
    {
        Collection<Resource> ret = new LinkedHashSet<>();
        Iterator<Resource> rscIt = nodeRef.iterateResources();
        while (rscIt.hasNext())
        {
            ret.add(rscIt.next());
        }
        return ret;
    }

    // storPool
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        StorPool storPoolRef,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> storPoolRef.getProps());
        // rscDfnListSupplier is (currently) not needed
        // builder.setRscDfnListSupplier(() -> getRscDfnsByStorPool(storPoolRef));
        builder.setRscListSupplier(() -> getRscsBy(storPoolRef));

        builder.addSkipDisk();
        builder.addLuksAllowDiscards();
        return builder.propsChangedListeners;
    }

    private Collection<Resource> getRscsBy(StorPool storPoolRef)
    {
        Collection<Resource> ret = new LinkedHashSet<>();
        for (VlmProviderObject<Resource> vlmData : storPoolRef.getVolumes())
        {
            ret.add(vlmData.getRscLayerObject().getAbsResource());
        }
        return ret;
    }

    // rsc
    public Map<String, PropertyChangedListener> buildPropsChangedListeners(
        Resource rscRef,
        List<Flux<ApiCallRc>> fluxes
    )
    {
        Builder builder = new Builder(fluxes);
        builder.setCurrentPropSupplier(() -> rscRef.getProps());
        builder.setRscDfnListSupplier(() -> Collections.singletonList(rscRef.getResourceDefinition()));
        builder.setRscListSupplier(() -> Collections.singleton(rscRef));

        builder.addSkipDisk();
        builder.addLuksAllowDiscards();
        return builder.propsChangedListeners;
    }

    private class Builder
    {
        private final List<Flux<ApiCallRc>> fluxes;
        private final Map<String, PropertyChangedListener> propsChangedListeners = new HashMap<>();

        private @Nullable Supplier<Props> currentPropSupplier;
        private @Nullable Supplier<Collection<ResourceDefinition>> rscDfnsSupplier;
        private @Nullable Supplier<Collection<Resource>> rscsSupplier;

        Builder(List<Flux<ApiCallRc>> fluxesRef)
        {
            fluxes = fluxesRef;
        }

        void setCurrentPropSupplier(Supplier<Props> currentPropSupplierRef)
        {
            currentPropSupplier = currentPropSupplierRef;
        }

        void setRscDfnListSupplier(
            Supplier<Collection<ResourceDefinition>> rscDfnsSupplierRef
        )
        {
            rscDfnsSupplier = rscDfnsSupplierRef;
        }

        void setRscListSupplier(
            Supplier<Collection<Resource>> rscsSupplierRef
        )
        {
            rscsSupplier = rscsSupplierRef;
        }

        Collection<Resource> buildRscListFromRscDfnSupplier()
        {
            List<Resource> ret = new ArrayList<>();
            for (ResourceDefinition rscDfn : rscDfnsSupplier.get())
            {
                Iterator<Resource> iterateResource = rscDfn.iterateResource();
                while (iterateResource.hasNext())
                {
                    ret.add(iterateResource.next());
                }
            }
            return ret;
        }

        void addDrbdOptionsDiskRsDiscardGranularity()
        {
            CtrlRscDfnApiCallHandler ctrlRscDfnHandler = ctrlRscDfnApiCallHandlerProvider.get();
            require(currentPropSupplier, "current property supplier");
            require(rscDfnsSupplier, "rscDfns supplier");
            propsChangedListeners.put(
                CtrlRscDfnApiCallHelper.FULL_KEY_RS_DISC_GRAN,
                (ignoredKey, newVal, oldValue) ->
                {
                    if (!Objects.equals(newVal, oldValue))
                    {
                        try
                        {
                            if (newVal != null)
                            {
                                // disable the auto-prop on the current level
                                currentPropSupplier.get()
                                    .setProp(
                                        ApiConsts.NAMESPC_DRBD_OPTIONS + "/" +
                                            ApiConsts.KEY_DRBD_AUTO_RS_DISCARD_GRANULARITY,
                                        ApiConsts.VAL_FALSE
                                    );
                            }
                            for (ResourceDefinition rscDfn : rscDfnsSupplier.get())
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
                        for (ResourceDefinition rscDfn : rscDfnsSupplier.get())
                        {
                            fluxes.add(ctrlRscDfnHandler.updateProps(rscDfn));
                        }
                    }
                }
            );
        }

        /**
         * If the users sets the regular {@value CtrlRscDfnApiCallHelper.FULL_KEY_DISCARD_GRAN} property on the current
         * propsContainer, we automatically disable Linstor/Drbd/auto-discard-granularity.
         */

        void addDrbdOptionsDiskDiscardGranularity()
        {
            CtrlRscDfnApiCallHandler ctrlRscDfnHandler = ctrlRscDfnApiCallHandlerProvider.get();
            require(currentPropSupplier, "current property supplier");
            require(rscDfnsSupplier, "rscDfns supplier");
            propsChangedListeners.put(
                CtrlRscDfnApiCallHelper.FULL_KEY_DISCARD_GRAN,
                (ignoredKey, newVal, oldValue) ->
                {
                    if (!Objects.equals(newVal, oldValue))
                    {
                        try
                        {
                            if (newVal != null)
                            {
                                // disable the auto-prop on the current level
                                currentPropSupplier.get()
                                    .setProp(
                                        ApiConsts.NAMESPC_LINSTOR_DRBD + "/" +
                                            ApiConsts.KEY_DRBD_AUTO_DISCARD_GRANULARITY,
                                        ApiConsts.VAL_FALSE
                                    );
                            }
                            for (ResourceDefinition rscDfn : rscDfnsSupplier.get())
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
            addDrbdOptionsAutoDiscardGranularity();
        }

        void addDrbdOptionsAutoDiscardGranularity()
        {
            require(currentPropSupplier, "current property supplier");
            require(rscDfnsSupplier, "rscDfns supplier");
            CtrlRscDfnApiCallHandler ctrlRscDfnHandler = ctrlRscDfnApiCallHandlerProvider.get();
            propsChangedListeners.put(
                ApiConsts.NAMESPC_LINSTOR_DRBD + "/" +
                    ApiConsts.KEY_DRBD_AUTO_DISCARD_GRANULARITY,
                (ignoredKey, newVal, oldValue) ->
                {
                    if (!Objects.equals(newVal, oldValue))
                    {
                        for (ResourceDefinition rscDfn : rscDfnsSupplier.get())
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

        void addLuksAllowDiscards()
        {
            CtrlRscDfnApiCallHandler ctrlRscDfnHandler = ctrlRscDfnApiCallHandlerProvider.get();
            propsChangedListeners.put(
                ApiConsts.NAMESPC_LUKS + "/" + ApiConsts.KEY_LUKS_ALLOW_DISCARDS,
                (ignoredKey, newVal, oldValue) ->
                {
                    if (!Objects.equals(newVal, oldValue))
                    {
                        Set<ResourceDefinition> rscDfnsToUpdate = new TreeSet<>();
                        if (rscDfnsSupplier != null)
                        {
                            for (ResourceDefinition rscDfn : rscDfnsSupplier.get())
                            {
                                rscDfnsToUpdate.add(rscDfn);
                            }
                        }
                        else if (rscsSupplier != null)
                        {
                            for (Resource rsc : rscsSupplier.get())
                            {
                                rscDfnsToUpdate.add(rsc.getResourceDefinition());
                            }
                        }
                        for (ResourceDefinition rscDfn : rscDfnsToUpdate)
                        {
                            fluxes.add(ctrlRscDfnHandler.updateProps(rscDfn));
                        }
                    }
                }
            );
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
                        for (Resource rsc : rscsSupplier.get())
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

        private void require(@Nullable Object requiredSupplier, String supplierDescrRef)
        {
            if (requiredSupplier == null)
            {
                throw new ImplementationError("Missing " + supplierDescrRef);
            }
        }
    }
}
