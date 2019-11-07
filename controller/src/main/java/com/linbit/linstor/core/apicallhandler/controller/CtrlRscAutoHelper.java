package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoTieBreakerHelper.AutoTiebreakerResult;
import com.linbit.linstor.core.apicallhandler.controller.internal.CtrlSatelliteUpdateCaller;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.CtrlResponseUtils;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscAutoHelper
{
    private final CtrlApiDataLoader dataLoader;
    private final CtrlRscAutoPlaceApiCallHandler autoPlaceHelper;
    private final CtrlRscAutoQuorumHelper autoQuorumHelper;
    private final CtrlRscAutoTieBreakerHelper autoTieBreakerHelper;
    private final Provider<AccessContext> peerAccCtx;
    private final CtrlRscCrtApiHelper rscCrtHelper;
    private final CtrlRscDeleteApiHelper rscDelHelper;
    private final CtrlRscToggleDiskApiCallHandler rscToggleDiskHelper;
    private final CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCaller;

    public static class AutoHelperResult
    {
        private Flux<ApiCallRc> flux;
        private boolean preventUpdateSatellitesForResourceDelete;

        private AutoHelperResult()
        {
        }

        public Flux<ApiCallRc> getFlux()
        {
            return flux;
        }

        public boolean isPreventUpdateSatellitesForResourceDelete()
        {
            return preventUpdateSatellitesForResourceDelete;
        }
    }

    @Inject
    public CtrlRscAutoHelper(
        CtrlRscAutoPlaceApiCallHandler autoPlaceHelperRef,
        CtrlRscAutoQuorumHelper autoQuorumHelperRef,
        CtrlRscAutoTieBreakerHelper autoTieBreakerRef,
        CtrlApiDataLoader dataLoaderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscCrtApiHelper rscCrtHelperRef,
        CtrlRscDeleteApiHelper rscDelHelperRef,
        CtrlRscToggleDiskApiCallHandler rscToggleDiskHelperRef,
        CtrlSatelliteUpdateCaller ctrlSatelliteUpdateCallerRef
    )
    {
        autoPlaceHelper = autoPlaceHelperRef;
        autoQuorumHelper = autoQuorumHelperRef;
        autoTieBreakerHelper = autoTieBreakerRef;

        dataLoader = dataLoaderRef;
        peerAccCtx = peerAccCtxRef;
        rscCrtHelper = rscCrtHelperRef;
        rscDelHelper = rscDelHelperRef;
        rscToggleDiskHelper = rscToggleDiskHelperRef;
        ctrlSatelliteUpdateCaller = ctrlSatelliteUpdateCallerRef;
    }

    /**
     * Returns an empty flux if no resources were added or deleted.
     * Returns the flux for creating and/or deleting the automatically managed resources.
     * <ul>
     * <li>a property on that resource has changed</li>
     * <li>the resource was already in deleting state</li>
     * </ul>
     *
     * @param apiCallRcImplRef
     * @param rscNameStrRef
     * @return
     */
    public AutoHelperResult manage(ApiCallRcImpl apiCallRcImplRef, ResponseContext context, String rscNameStrRef)
    {
        return manage(apiCallRcImplRef, context, dataLoader.loadRscDfn(rscNameStrRef, true));
    }

    public AutoHelperResult manage(
        ApiCallRcImpl apiCallRcImpl,
        ResponseContext context,
        ResourceDefinition rscDfn
    )
    {
        return manage(apiCallRcImpl, context, rscDfn, Collections.emptySet());
    }

    public AutoHelperResult manage(
        ApiCallRcImpl apiCallRcImpl,
        ResponseContext context,
        ResourceDefinition rscDfn,
        Set<Resource> candidatesForTakeover
    )
    {
        AutoHelperResult result = new AutoHelperResult();

        TreeSet<Resource> resourcesToCreate = new TreeSet<>();
        TreeSet<NodeName> nodeNamesForDelete = new TreeSet<>();

        boolean requiresUpdateFlux = false;
        boolean fluxUpdateApplied = false;

        Flux<ApiCallRc> flux = Flux.empty();

        AutoTiebreakerResult tiebreakerResult = autoTieBreakerHelper.manage(
            apiCallRcImpl,
            rscDfn,
            candidatesForTakeover
        );
        if (tiebreakerResult.created != null)
        {
            resourcesToCreate.add(tiebreakerResult.created);
            requiresUpdateFlux = true;
        }
        if (tiebreakerResult.deleting != null)
        {
            nodeNamesForDelete.add(tiebreakerResult.deleting.getAssignedNode().getName());
            requiresUpdateFlux = true;
        }
        if (tiebreakerResult.takeoverDiskless != null)
        {
            flux = autoTieBreakerHelper.setTiebreakerFlag(tiebreakerResult.takeoverDiskless);
            requiresUpdateFlux = true;
            result.preventUpdateSatellitesForResourceDelete = true;
        }
        if (tiebreakerResult.takeoverDiskful != null)
        {
            flux = rscToggleDiskHelper.resourceToggleDisk(
                tiebreakerResult.takeoverDiskful.getAssignedNode().getName().displayValue,
                rscDfn.getName().displayValue,
                null,
                null,
                true
            ).concatWith(autoTieBreakerHelper.setTiebreakerFlag(tiebreakerResult.takeoverDiskful));

            result.preventUpdateSatellitesForResourceDelete = true;
            requiresUpdateFlux = true;
            fluxUpdateApplied = true;
        }

        autoQuorumHelper.manage(apiCallRcImpl, rscDfn);

        if (!resourcesToCreate.isEmpty())
        {
            flux = flux.concatWith(rscCrtHelper.deployResources(context, resourcesToCreate));
            fluxUpdateApplied = true;
        }
        if (!nodeNamesForDelete.isEmpty())
        {
            flux = flux.concatWith(
                rscDelHelper.updateSatellitesForResourceDelete(
                    nodeNamesForDelete,
                    rscDfn.getName()
                )
            );
            fluxUpdateApplied = true;
        }

        if (requiresUpdateFlux && !fluxUpdateApplied)
        {
            flux = flux.concatWith(
                ctrlSatelliteUpdateCaller.updateSatellites(rscDfn, Flux.empty())
                    .transform(
                        updateResponses -> CtrlResponseUtils.combineResponses(
                            updateResponses,
                            rscDfn.getName(),
                            "Resource {1} updated on node {0}"
                        )
                    )
            );
        }

        result.flux = flux;
        return result;
    }

    public Resource getTiebreakerResource(String nodeNameRef, String nameRef)
    {
        Resource ret = null;
        Resource rsc = dataLoader.loadRsc(nodeNameRef, nameRef, false);
        try
        {
            if (rsc != null && rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.Flags.TIE_BREAKER))
            {
                ret = rsc;
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "check if given resource is a tiebreaker",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return ret;
    }

    public void removeTiebreakerFlag(Resource tiebreakerRef)
    {
        try
        {
            StateFlags<Flags> flags = tiebreakerRef.getStateFlags();
            flags.disableFlags(peerAccCtx.get(), Resource.Flags.TIE_BREAKER);
            flags.enableFlags(peerAccCtx.get(), Resource.Flags.DRBD_DISKLESS);
        }
        catch (AccessDeniedException exc)
        {
            throw new ApiAccessDeniedException(
                exc,
                "remove tiebreaker flag from resource",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags flag)
    {
        boolean isFlagSet;
        try
        {
            isFlagSet = rsc.getStateFlags().isSet(peerAccCtx.get(), flag);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "checking flag state of " + rsc,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return isFlagSet;
    }

    private Resource findTiebreaker(ResourceDefinition rscDfn)
    {
        Resource tiebreaker;
        try
        {
            tiebreaker = rscDfn.streamResource(peerAccCtx.get())
                .filter(rsc -> isFlagSet(rsc, Resource.Flags.TIE_BREAKER))
                .findAny()
                .orElse(null);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "finding tiebreaker resource of " + rscDfn,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return tiebreaker;
    }
}