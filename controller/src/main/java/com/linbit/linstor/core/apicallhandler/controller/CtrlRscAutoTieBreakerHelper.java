package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.layer.LayerRscUtils;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.api.ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
import static com.linbit.linstor.api.ApiConsts.NAMESPC_DRBD_OPTIONS;
import static com.linbit.linstor.api.ApiConsts.VAL_FALSE;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscAutoTieBreakerHelper implements CtrlRscAutoHelper.AutoHelper
{
    private final SystemConfRepository systemConfRepository;
    private final CtrlRscLayerDataFactory layerDataHelper;
    private final NodeRepository nodeRepo;
    private final CtrlRscCrtApiHelper rscCrtApiHelper;
    private final Provider<AccessContext> peerCtx;
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ResponseConverter responseConverter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscToggleDiskApiCallHandler rscToggleDiskHelper;
    private final Autoplacer autoplacer;

    class AutoTiebreakerResult
    {
        /**
         * Set if a new tiebreaker resource was created
         */
        Resource created = null;
        /**
         * Set if a linstor managed tiebreaker is deleted by autoTiebreaker mechanism
         */
        Resource deleting = null;
        /**
         * Set if the linstor (now) managed tiebreaker needs to be updated. This could be the
         * case when taking over a previously diskless resource as a now tiebreaking resource (i.e.
         * making a non-linstor-managed diskless resource to a linstor-managed diskless resource ->
         * tiebreaker)
         */
        Resource takeoverDiskless = null;

        /**
         * Set if tiebreaker needs a toggle disk. Currently only used if a previously non-linstor managed
         * diskful resource in deleting state is being taken over, resulting in a diskless linstor managed
         * tiebreaker resource.
         */
        Resource takeoverDiskful = null;
    }

    @Inject
    public CtrlRscAutoTieBreakerHelper(
        SystemConfRepository systemConfRepositoryRef,
        ScopeRunner scopeRunnerRef,
        NodeRepository nodeRepoRef,
        CtrlRscLayerDataFactory layerDataHelperRef,
        @PeerContext Provider<AccessContext> peerCtxRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlRscCrtApiHelper rscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscToggleDiskApiCallHandler rscToggleDiskHelperRef,
        Autoplacer autoplacerRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        scopeRunner = scopeRunnerRef;
        nodeRepo = nodeRepoRef;
        layerDataHelper = layerDataHelperRef;
        peerCtx = peerCtxRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscCrtApiHelper = rscCrtApiHelperRef;
        responseConverter = responseConverterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        rscToggleDiskHelper = rscToggleDiskHelperRef;
        autoplacer = autoplacerRef;
    }

    @Override
    public void manage(AutoHelperContext ctx)
    {
        try
        {
            if (isAutoTieBreakerEnabled(ctx.rscDfn))
            {
                Resource tieBreaker = getTieBreaker(ctx.rscDfn);
                if (shouldTieBreakerExist(ctx.rscDfn))
                {
                    if (tieBreaker == null)
                    {
                        Resource takeover = null;
                        Iterator<Resource> rscIt = ctx.rscDfn.iterateResource(peerCtx.get());
                        while (rscIt.hasNext())
                        {
                            Resource rsc = rscIt.next();
                            if (isFlagSet(rsc, Resource.Flags.DELETE))
                            {
                                if (isFlagSet(rsc, Resource.Flags.DRBD_DISKLESS))
                                {
                                    takeover = rsc;
                                    break;
                                }
                                else
                                {
                                    takeover = rsc;
                                    // keep looking for diskless resource
                                }
                            }
                        }
                        if (takeover != null)
                        {
                            takeover(takeover, ctx);
                        }
                        else
                        {
                            StorPool storPool = getStorPoolForTieBreaker(ctx);
                            if (storPool == null)
                            {
                                ctx.responses.addEntries(
                                    ApiCallRcImpl.singleApiCallRc(
                                        ApiConsts.WARN_NOT_ENOUGH_NODES_FOR_TIE_BREAKER,
                                        String.format(
                                            "Could not find suitable node to automatically create a tie breaking resource for '%s'.",
                                            ctx.rscDfn.getName().displayValue
                                        )
                                    )
                                );
                            }
                            else
                            {
                                // we can ignore the .objA of the returned pair as we are just about to create
                                // a tiebreaker
                                tieBreaker = rscCrtApiHelper.createResourceDb(
                                    storPool.getNode().getName().displayValue,
                                    ctx.rscDfn.getName().displayValue,
                                    Resource.Flags.TIE_BREAKER.flagValue,
                                    Collections.singletonMap(
                                        ApiConsts.KEY_STOR_POOL_NAME,
                                        storPool.getName().displayValue
                                    ),
                                    Collections.emptyList(),
                                    null,
                                    Collections.emptyMap(),
                                    Collections.emptyList()
                                ).objB.extractApiCallRc(ctx.responses);

                                ctx.responses.addEntries(
                                    ApiCallRcImpl.singleApiCallRc(
                                        ApiConsts.INFO_TIE_BREAKER_CREATED,
                                        String.format("Tie breaker resource '%s' created on %s",
                                            ctx.rscDfn.getName().displayValue,
                                            storPool.getName().displayValue
                                        )
                                    )
                                );

                                ctx.resourcesToCreate.add(tieBreaker);
                                ctx.requiresUpdateFlux = true;
                            }
                        }
                    }
                    else
                    {
                        if (isFlagSet(tieBreaker, Resource.Flags.DELETE))
                        {
                            // user requested to delete tiebreaker.
                            tieBreaker.getDefinition().getProps(peerCtx.get()).setProp(
                                KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER,
                                VAL_FALSE,
                                NAMESPC_DRBD_OPTIONS
                            );
                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.INFO_PROP_SET,
                                    "Disabling auto-tiebreaker on resource-definition '"
                                        + tieBreaker.getDefinition().getName() +
                                        "' as tiebreaker resource was manually deleted"
                                )
                            );
                        }
                    }
                }
                else
                {
                    if (tieBreaker != null)
                    {
                        tieBreaker.markDeleted(peerCtx.get());
                        ctx.responses.addEntries(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.INFO_TIE_BREAKER_DELETING,
                                "Tie breaker marked for deletion"
                            )
                        );

                        ctx.nodeNamesForDelete.add(tieBreaker.getNode().getName());
                        ctx.requiresUpdateFlux = true;
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "managing auto-quorum feature " + getRscDfnDescriptionInline(ctx.rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private void takeover(
        Resource rsc,
        AutoHelperContext ctx
    )
    {
        StateFlags<Flags> flags = rsc.getStateFlags();
        AccessContext accCtx = peerCtx.get();
        try
        {
            flags.disableFlags(accCtx, Resource.Flags.DELETE);

            Iterator<Volume> vlmsIt = rsc.iterateVolumes();
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();
                vlm.getFlags().disableFlags(accCtx, Volume.Flags.DELETE);
            }

            if (flags.isSet(accCtx, Resource.Flags.DRBD_DISKLESS))
            {
                ctx.additionalFluxList.add(setTiebreakerFlag(rsc));
                ctx.requiresUpdateFlux = true;
                ctx.preventUpdateSatellitesForResourceDelete = true;
            }
            else
            {
                ctx.additionalFluxList.add(
                    rscToggleDiskHelper.resourceToggleDisk(
                        rsc.getNode().getName().displayValue,
                        rsc.getDefinition().getName().displayValue,
                        null,
                        null,
                        true
                    ).concatWith(setTiebreakerFlag(rsc))
                );

                ctx.preventUpdateSatellitesForResourceDelete = true;
                ctx.requiresUpdateFlux = false; // resourceToggleDisk already performs stltUpdate
            }
            ctx.responses.addEntries(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.INFO_TIE_BREAKER_TAKEOVER,
                    "The given resource will not be deleted but will be taken over as a linstor managed tiebreaker resource."
                )
            );
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "accessing flags of " + getRscDescriptionInline(rsc),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private boolean isAutoTieBreakerEnabled(ResourceDefinition rscDfn)
    {
        boolean autoTieBreakerEnabled;
        try
        {
            String autoTieBreakerProp = getPrioProps(rscDfn)
                .getProp(KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER, NAMESPC_DRBD_OPTIONS);
            autoTieBreakerEnabled = ApiConsts.VAL_TRUE.equalsIgnoreCase(autoTieBreakerProp);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "checking auto-quorum / auto-tiebreaker feature " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return autoTieBreakerEnabled;
    }

    private Resource getTieBreaker(ResourceDefinition rscDfn)
    {
        Resource tieBreaker = null;
        try
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(peerCtx.get());
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();
                if (rsc.getStateFlags().isSet(peerCtx.get(), Resource.Flags.TIE_BREAKER))
                {
                    tieBreaker = rsc;
                    break;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access resources of resource definition " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return tieBreaker;
    }

    private boolean shouldTieBreakerExist(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        boolean hasEveryoneEnoughPeerSlots = true;
        long diskfulDrbdCount = 0;
        long disklessDrbdCount = 0;

        AccessContext peerAccCtx = peerCtx.get();
        int currentCount = rscDfn.getResourceCount();

        for (RscDfnLayerObject rscDfnData : rscDfn.getLayerData(peerAccCtx, DeviceLayerKind.DRBD).values())
        {
            if (((DrbdRscDfnData<Resource>) rscDfnData).getPeerSlots() <= currentCount)
            {
                hasEveryoneEnoughPeerSlots = false;
                break;
            }
        }

        if (CtrlRscAutoQuorumHelper.isAutoQuorumEnabled(getPrioProps(rscDfn)))
        {
            Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();

                StateFlags<Resource.Flags> rscFlags = rsc.getStateFlags();
                if (
                    layerDataHelper.getLayerStack(rsc).contains(DeviceLayerKind.DRBD) &&
                    rscFlags.isUnset(peerAccCtx, Resource.Flags.DELETE)
                )
                {
                    if (rscFlags.isSet(peerAccCtx, Resource.Flags.DRBD_DISKLESS))
                    {
                        if (!rscFlags.isSet(peerAccCtx, Resource.Flags.TIE_BREAKER))
                        {
                            // do not count tiebreaker here as this method should determine if the tiebreaker should
                            // exist or not
                            disklessDrbdCount++;
                        }
                    }
                    else
                    {
                        diskfulDrbdCount++;
                    }
                }

                AbsRscLayerObject<Resource> layerData = rsc.getLayerData(peerAccCtx);
                Set<AbsRscLayerObject<Resource>> drbdDataSet = LayerRscUtils.getRscDataByProvider(
                    layerData,
                    DeviceLayerKind.DRBD
                );
                for (AbsRscLayerObject<Resource> rlo : drbdDataSet)
                {
                    if (((DrbdRscData<Resource>) rlo).getPeerSlots() <= currentCount)
                    {
                        hasEveryoneEnoughPeerSlots = false;
                        break;
                    }
                }
            }
        }
        // TODO: maybe change to something like diskful % 2==0 && diskless % 2==0 && diskful > 0
        return hasEveryoneEnoughPeerSlots && diskfulDrbdCount == 2 && disklessDrbdCount == 0;
    }

    private PriorityProps getPrioProps(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        AccessContext accCtx = peerCtx.get();
        return new PriorityProps(
            rscDfn.getProps(accCtx),
            rscDfn.getResourceGroup().getProps(accCtx),
            systemConfRepository.getCtrlConfForView(accCtx)
        );
    }

    private StorPool getStorPoolForTieBreaker(AutoHelperContext ctx)
    {
        StorPool storPool = null;

        try
        {
            AccessContext peerAccCtx = peerCtx.get();

            List<String> filterNodeNamesList = new ArrayList<>();
            while (storPool == null)
            {
                Optional<Set<StorPool>> autoplaceResult = autoplacer.autoPlace(
                    AutoSelectFilterPojo.merge(
                        new AutoSelectFilterPojo(
                            0,
                            1,
                            filterNodeNamesList,
                            null,
                            Collections.singletonList(ctx.rscDfn.getName().displayValue),
                            null,
                            null,
                            null,
                            Collections.singletonList(DeviceLayerKind.DRBD),
                            null,
                            null,
                            null,
                            Resource.Flags.DRBD_DISKLESS.name()
                        ),
                        ctx.rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData(),
                        ctx.selectFilter
                    ),
                    ctx.rscDfn,
                    0 // doesn't matter as we are diskless
                );

                if (autoplaceResult.isPresent() && !autoplaceResult.get().isEmpty())
                {
                    storPool = autoplaceResult.get().iterator().next();
                    if (!supportsTieBreaker(peerAccCtx, storPool.getNode()))
                    {
                        /*
                         * autoplacer only checks if DRBD >= 9 is supported, but we need >= 9.0.19
                         */
                        filterNodeNamesList.add(storPool.getNode().getName().displayValue);
                        storPool = null;
                    }
                }
                else
                {
                    break;
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access nodes map ",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return storPool;
    }

    private boolean supportsTieBreaker(AccessContext peerAccCtx, Node node) throws AccessDeniedException
    {
        return node.getPeer(peerAccCtx)
            .getExtToolsManager()
            .getExtToolInfo(ExtTools.DRBD9)
            .isSupportedAndHasVersionOrHigher(new ExtToolsInfo.Version(9, 0, 19));
    }

    private boolean isFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isFlagSet;
        try
        {
            isFlagSet = rsc.getStateFlags().isSet(peerCtx.get(), flags);
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

    /**
     * Sets the tiebreaker flag in a transaction and commits the transaction.
     * Does NOT update satellites
     *
     * @param tiebreaker
     *
     * @return
     */
    public Flux<ApiCallRc> setTiebreakerFlag(Resource tiebreaker)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeModifyOperation(),
            tiebreaker.getNode().getName().getDisplayName(),
            tiebreaker.getDefinition().getName().getDisplayName()
        );

        return scopeRunner
            .fluxInTransactionalScope(
                "Setting tiebreaker flag",
                LockGuard.createDeferred(nodesMapLock.writeLock(), rscDfnMapLock.writeLock()),
                () -> setTiebreakerFlagInTransaction(tiebreaker, context)
            )
            .transform(responses -> responseConverter.reportingExceptions(context, responses));
    }

    private Flux<ApiCallRc> setTiebreakerFlagInTransaction(Resource tiebreakerRef, ResponseContext contextRef)
    {
        try
        {
            tiebreakerRef.getStateFlags().enableFlags(peerCtx.get(), Resource.Flags.TIE_BREAKER);

            ctrlTransactionHelper.commit();
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "marking resource as tiebreaker " + tiebreakerRef,
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
        return Flux.empty();
    }
}
