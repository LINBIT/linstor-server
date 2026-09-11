package com.linbit.linstor.core.apicallhandler.controller.autohelper;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiDataLoader;
import com.linbit.linstor.core.apicallhandler.controller.CtrlResyncAfterHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscCrtApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscDeleteApiHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlTransactionHelper;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.ResourceDefinitionRepositoryImpl;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.locks.LockGuardFactory;
import com.linbit.locks.LockGuardFactory.LockObj;
import com.linbit.locks.LockGuardFactory.LockType;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import reactor.core.publisher.Flux;

/**
 * Main class for managing all AutoHelpers.
 *
 * <p>There is a central {@link #manage(AutoHelperContext)} method that automatically runs all known AutoHelpers
 *  for the given ResourceDefinitions. If only one or a few specific AutoHelpers should be executed, use
 *  {@link #manage(AutoHelperContext, Set)}</p>
 */
@Singleton
public class CtrlRscAutoHelper
{
    private static final Set<AutoHelperType> ALL_AUTO_HELPER_TYPE_SET;

    static
    {
        ALL_AUTO_HELPER_TYPE_SET = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AutoHelperType.values())));
    }

    private final ErrorReporter errorReporter;
    private final CtrlApiDataLoader dataLoader;
    private final CtrlRscCrtApiHelper rscCrtHelper;
    private final CtrlRscDeleteApiHelper rscDelHelper;
    private final CtrlResyncAfterHelper resyncAfterHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    // Do NOT convert to EnumMap. Although it is tempting, we want to make sure to manually manage a certain
    // order of autoHelpers (i.e. tiebreaker + quorum last).
    private final List<AutoHelper> autohelperList;
    private final ScopeRunner scopeRunner;
    private final LockGuardFactory lockGuardFactory;
    private final ResourceDefinitionRepositoryImpl rscDfnRepo;
    private final CtrlTransactionHelper ctrlTxHelper;

    @Inject
    public CtrlRscAutoHelper(
        CtrlRscAutoQuorumHelper autoQuorumHelperRef,
        CtrlRscAutoTieBreakerHelper autoTieBreakerRef,
        CtrlRscAutoDrbdProxyHelper autoDrbdProxyHelperRef,
        CtrlRscAutoRePlaceRscHelper autoRePlaceRscHelperRef,
        CtrlRscDfnAutoVerifyAlgoHelper autoVerifyAlgoHelperRef,
        CtrlResyncAfterHelper resyncAfterHelperRef,
        CtrlApiDataLoader dataLoaderRef,
        CtrlRscCrtApiHelper rscCrtHelperRef,
        CtrlRscDeleteApiHelper rscDelHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef,
        ScopeRunner scopeRunnerRef,
        LockGuardFactory lockGuardFactoryRef,
        ResourceDefinitionRepositoryImpl rscDfnRepoRef,
        CtrlTransactionHelper ctrlTxHelperRef,
        ErrorReporter errorReporterRef
    )
    {
        ctrlTxHelper = ctrlTxHelperRef;
        autohelperList = Arrays
            .asList(
                autoDrbdProxyHelperRef,
                autoRePlaceRscHelperRef,
                autoVerifyAlgoHelperRef,
                // run autotiebreaker + autoquorum as last
                autoTieBreakerRef,
                autoQuorumHelperRef
            );

        dataLoader = dataLoaderRef;
        rscCrtHelper = rscCrtHelperRef;
        rscDelHelper = rscDelHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
        scopeRunner = scopeRunnerRef;
        lockGuardFactory = lockGuardFactoryRef;
        rscDfnRepo = rscDfnRepoRef;
        resyncAfterHelper = resyncAfterHelperRef;
        errorReporter = errorReporterRef;
    }

    public AutoHelperResult manage(ApiCallRcImpl apiCallRcImplRef, ResponseContext context, String rscNameStrRef)
    {
        return manage(new AutoHelperContext(apiCallRcImplRef, context, dataLoader.loadRscDfn(rscNameStrRef)));
    }

    public Flux<ApiCallRc> manageAll(AutoHelperContext autoCtxWithoutRscDfn)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Create storage pool",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
            () -> manageAllInTransaction(autoCtxWithoutRscDfn)
        );
    }

    private Flux<ApiCallRc> manageAllInTransaction(AutoHelperContext autoCtxWithoutRscDfn)
    {
        List<Flux<ApiCallRc>> fluxList = new ArrayList<>();
        for (ResourceDefinition rscDfn : rscDfnRepo.getMapForView().values())
        {
            AutoHelperResult result = manage(
                new AutoHelperContext(
                    autoCtxWithoutRscDfn.responses,
                    autoCtxWithoutRscDfn.responseContext,
                    rscDfn
                )
            );
            fluxList.add(result.flux());
        }
        ctrlTxHelper.commit();
        return Flux.merge(fluxList);
    }

    public Flux<ApiCallRc> manageInOwnTransaction(AutoHelperContext autoCtx)
    {
        return scopeRunner.fluxInTransactionalScope(
            "Create storage pool",
            lockGuardFactory.buildDeferred(LockType.WRITE, LockObj.NODES_MAP, LockObj.RSC_DFN_MAP),
            () -> manage(autoCtx).flux()
        );
    }

    public AutoHelperResult manage(AutoHelperContext ctx)
    {
        return manage(ctx, ALL_AUTO_HELPER_TYPE_SET);
    }

    public AutoHelperResult manage(AutoHelperContext ctx, AutoHelperType... typeFilters)
    {
        return manage(ctx, new HashSet<>(Arrays.asList(typeFilters)));
    }

    public AutoHelperResult manage(AutoHelperContext ctx, Set<AutoHelperType> typeFilter)
    {
        for (AutoHelper autohelper : autohelperList)
        {
            if (typeFilter.contains(autohelper.getType()))
            {
                autohelper.manage(ctx);
            }
        }

        ctx.additionalFluxList.add(resyncAfterHelper.fluxManage());

        if (!ctx.resourcesToCreate.isEmpty())
        {
            ctx.additionalFluxList.add(
                rscCrtHelper.deployResources(
                    ctx.responseContext,
                    ctx.resourcesToCreate
                )
            );
        }

        if (!ctx.nodeNamesForDelete.isEmpty())
        {
            ctx.additionalFluxList.add(
                rscDelHelper.updateSatellitesForResourceDelete(
                    ctx.responseContext,
                    ctx.nodeNamesForDelete,
                    ctx.rscDfn.getName()
                )
            );
        }

        Flux<ApiCallRc> flux = Flux.merge(ctx.additionalFluxList);

        if (ctx.requiresUpdateFlux)
        {
            // if an AutoHelper set requiresUpdateFlux we usually want that first all auto-fluxes finished
            // and after that one updateSatellites is send out. Having this updateSatellite in the same
            // Flux.merge block would allow a race there the updateSatellite flux is subscribed to before
            // some AutoHelper made some changes / commits that should have been included in the updateSatellites.
            // the easiest example for this is if one AutoHelper simply has a chain of fluxes and only a later flux
            // step actually makes some persistent changes that updateSatellites should send to the satellites.

            // hence we need to wait until all additionalFluxList finished before sending out one final
            // updateSatellites.
            flux = flux.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(ctx.rscDfn, Flux.empty())
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            errorReporter,
                            updateResponses,
                            ctx.rscDfn.getName(),
                            "Resource {1} updated on node {0}"
                        )
                    )
            );
        }

        return new AutoHelperResult(
            flux,
            ctx.responses,
            ctx.preventUpdateSatellitesForResourceDelete
        );
    }
}
