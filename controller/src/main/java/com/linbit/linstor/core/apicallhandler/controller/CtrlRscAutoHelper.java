package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

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

    @Inject
    public CtrlRscAutoHelper(
        CtrlRscAutoPlaceApiCallHandler autoPlaceHelperRef,
        CtrlRscAutoQuorumHelper autoQuorumHelperRef,
        CtrlRscAutoTieBreakerHelper autoTieBreakerRef,
        CtrlApiDataLoader dataLoaderRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        CtrlRscCrtApiHelper rscCrtHelperRef,
        CtrlRscDeleteApiHelper rscDelHelperRef
    )
    {
        autoPlaceHelper = autoPlaceHelperRef;
        autoQuorumHelper = autoQuorumHelperRef;
        autoTieBreakerHelper = autoTieBreakerRef;

        dataLoader = dataLoaderRef;
        peerAccCtx = peerAccCtxRef;
        rscCrtHelper = rscCrtHelperRef;
        rscDelHelper = rscDelHelperRef;
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
    public Flux<ApiCallRc> manage(ApiCallRcImpl apiCallRcImplRef, ResponseContext context, String rscNameStrRef)
    {
        return manage(apiCallRcImplRef, context, dataLoader.loadRscDfn(rscNameStrRef, true));
    }

    public Flux<ApiCallRc> manage(ApiCallRcImpl apiCallRcImpl, ResponseContext context, ResourceDefinition rscDfn)
    {
        Flux<ApiCallRc> flux = null;

        Resource tieBreakerRsc = autoTieBreakerHelper.manage(apiCallRcImpl, rscDfn);
        TreeSet<Resource> resourcesToCreate = new TreeSet<>();
        TreeSet<NodeName> nodeNamesForDelete = new TreeSet<>();
        if (tieBreakerRsc != null)
        {
            if (isMarkedForDeletion(tieBreakerRsc))
            {
                nodeNamesForDelete.add(tieBreakerRsc.getAssignedNode().getName());
            }
            else
            {
                resourcesToCreate.add(tieBreakerRsc);
            }
        }
        autoQuorumHelper.manage(apiCallRcImpl, rscDfn);

        if (!resourcesToCreate.isEmpty())
        {
            flux = rscCrtHelper.deployResources(context, resourcesToCreate);
        }
        if (!nodeNamesForDelete.isEmpty())
        {
            Flux<ApiCallRc> deleteFlux = rscDelHelper.updateSatellitesForResourceDelete(
                nodeNamesForDelete,
                rscDfn.getName()
            );
            if (flux == null)
            {
                flux = deleteFlux;
            }
            else
            {
                flux.concatWith(deleteFlux);
            }
        }

        return flux == null ? Flux.empty() : flux;
    }

    private boolean isMarkedForDeletion(Resource rsc)
    {
        boolean isMarkedForDeletion;
        try
        {
            isMarkedForDeletion = rsc.getStateFlags().isSet(peerAccCtx.get(), Resource.Flags.DELETE);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "check deleted status of " + rsc,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return isMarkedForDeletion;
    }
}