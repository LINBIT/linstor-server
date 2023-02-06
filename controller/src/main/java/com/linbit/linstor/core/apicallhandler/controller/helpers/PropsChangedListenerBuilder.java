package com.linbit.linstor.core.apicallhandler.controller.helpers;

import com.linbit.ImplementationError;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlPropsHelper.PropertyChangedListener;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHelper;
import com.linbit.linstor.core.apicallhandler.utils.LinstorIteratorUtils;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.core.repository.ResourceDefinitionProtectionRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ExceptionThrowingSupplier;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import reactor.core.publisher.Flux;

@Singleton
public class PropsChangedListenerBuilder
{
    private final Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerProvider;
    private final SystemConfRepository systemConfRepository;
    private final ResourceDefinitionProtectionRepository rscDfnProtRepo;

    @Inject
    public PropsChangedListenerBuilder(
        Provider<CtrlRscDfnApiCallHandler> ctrlRscDfnApiCallHandlerProviderRef,
        SystemConfRepository systemConfRepositoryRef,
        ResourceDefinitionProtectionRepository rscDfnProtRepoRef
    )
    {
        ctrlRscDfnApiCallHandlerProvider = ctrlRscDfnApiCallHandlerProviderRef;
        systemConfRepository = systemConfRepositoryRef;
        rscDfnProtRepo = rscDfnProtRepoRef;
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

        builder.addDrbdOptionsDiskRsDiscardGranularity();
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

        builder.addDrbdOptionsDiskRsDiscardGranularity();
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

        builder.addDrbdOptionsDiskRsDiscardGranularity();
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

        builder.addDrbdOptionsDiskRsDiscardGranularity();
        return builder.propsChangedListeners;
    }

    private class Builder
    {
        private final List<Flux<ApiCallRc>> fluxes;
        private final Map<String, PropertyChangedListener> propsChangedListeners = new HashMap<>();

        private ExceptionThrowingSupplier<Props, AccessDeniedException> currentPropSupplier;
        private ExceptionThrowingSupplier<Collection<ResourceDefinition>, AccessDeniedException> rscDfnsSupplier;

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

        private void require(Object requiredSupplier, String supplierDescrRef)
        {
            if (requiredSupplier == null)
            {
                throw new ImplementationError("Missing " + supplierDescrRef);
            }
        }
    }
}
