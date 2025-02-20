package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.AutoSelectFilterPojo;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.StorPoolDefinitionMap;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.SelectionManager;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils.DrbdResourceResult;
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
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.locks.LockGuard;

import static com.linbit.linstor.api.ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
import static com.linbit.linstor.api.ApiConsts.NAMESPC_DRBD_OPTIONS;
import static com.linbit.linstor.api.ApiConsts.VAL_FALSE;
import static com.linbit.linstor.api.ApiConsts.VAL_TRUE;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscApiCallHandler.getRscDescriptionInline;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Predicate;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscAutoTieBreakerHelper implements CtrlRscAutoHelper.AutoHelper
{
    private final SystemConfRepository systemConfRepository;
    private final CtrlRscLayerDataFactory layerDataHelper;
    private final NodeRepository nodeRepo;
    private final StorPoolDefinitionMap storPoolDfnMap;
    private final CtrlRscCrtApiHelper rscCrtApiHelper;
    private final Provider<AccessContext> peerCtx;
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ResponseConverter responseConverter;
    private final CtrlTransactionHelper ctrlTransactionHelper;
    private final CtrlRscToggleDiskApiCallHandler rscToggleDiskHelper;
    private final Autoplacer autoplacer;

    private final ErrorReporter errorReporter;

    @Inject
    public CtrlRscAutoTieBreakerHelper(
        SystemConfRepository systemConfRepositoryRef,
        ScopeRunner scopeRunnerRef,
        NodeRepository nodeRepoRef,
        StorPoolDefinitionMap storPoolDfnMapRef,
        CtrlRscLayerDataFactory layerDataHelperRef,
        @PeerContext Provider<AccessContext> peerCtxRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlRscCrtApiHelper rscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef,
        CtrlRscToggleDiskApiCallHandler rscToggleDiskHelperRef,
        Autoplacer autoplacerRef,
        ErrorReporter errorReporterRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        scopeRunner = scopeRunnerRef;
        nodeRepo = nodeRepoRef;
        storPoolDfnMap = storPoolDfnMapRef;
        layerDataHelper = layerDataHelperRef;
        peerCtx = peerCtxRef;
        nodesMapLock = nodesMapLockRef;
        rscDfnMapLock = rscDfnMapLockRef;
        rscCrtApiHelper = rscCrtApiHelperRef;
        responseConverter = responseConverterRef;
        ctrlTransactionHelper = ctrlTransactionHelperRef;
        rscToggleDiskHelper = rscToggleDiskHelperRef;
        autoplacer = autoplacerRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public void manage(@Nonnull AutoHelperContext ctx)
    {
        try
        {
            boolean isAutoTieBreakerEnabled = isAutoTieBreakerEnabled(ctx.rscDfn);
            if (!isAutoTieBreakerEnabled && ctx.keepTiebreaker)
            {
                enableAutoTieBreaker(ctx);
                isAutoTieBreakerEnabled = true;
            }
            @Nullable Resource tieBreaker = getTieBreaker(ctx.rscDfn);
            boolean shouldTieBreakerExist = isAutoTieBreakerEnabled && shouldTieBreakerExist(ctx.rscDfn);
            if (shouldTieBreakerExist)
            {
                if (tieBreaker == null)
                {
                    Resource takeover = null;
                    Iterator<Resource> rscIt = ctx.rscDfn.iterateResource(peerCtx.get());
                    while (rscIt.hasNext())
                    {
                        Resource rsc = rscIt.next();
                        if (isSomeFlagSet(rsc, Resource.Flags.DRBD_DELETE, Resource.Flags.DELETE) &&
                            !isSomeFlagSet(rsc.getNode(), Node.Flags.EVICTED, Node.Flags.EVACUATE) &&
                            couldTakeover(rsc, ctx))
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
                        StorPool storPool = getStorPoolForTieBreaker(ctx, null);
                        if (storPool == null)
                        {
                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.WARN_NOT_ENOUGH_NODES_FOR_TIE_BREAKER,
                                    String.format(
                                        "Could not find suitable node to automatically create a tie breaking " +
                                            "resource for '%s'.",
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
                                Collections.emptyList(),
                                null
                            ).objB.extractApiCallRc(ctx.responses);

                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.INFO_TIE_BREAKER_CREATED,
                                    String.format(
                                        "Tie breaker resource '%s' created on %s",
                                        ctx.rscDfn.getName().displayValue,
                                        storPool.getNode().getName().displayValue
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
                    if (isFlagSet(tieBreaker, Resource.Flags.DRBD_DELETE))
                    {
                        // user requested to delete tiebreaker.
                        if (ctx.keepTiebreaker)
                        {
                            // however, --keep-tiebreaker overrules this deletion now
                            // the takeover will remove the DELETE flags
                            takeover(tieBreaker, ctx);
                        }
                        else
                        {
                            tieBreaker.getResourceDefinition()
                                .getProps(peerCtx.get())
                                .setProp(
                                    KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER,
                                    VAL_FALSE,
                                    NAMESPC_DRBD_OPTIONS
                                );
                            ctx.responses.addEntries(
                                ApiCallRcImpl.singleApiCallRc(
                                    ApiConsts.INFO_PROP_SET,
                                    "Disabling auto-tiebreaker on resource-definition '" +
                                        tieBreaker.getResourceDefinition().getName() +
                                        "' as tiebreaker resource was manually deleted"
                                )
                            );
                        }
                    }
                }
            }
            else
            {
                if (tieBreaker != null)
                {
                    // this cannot be the last diskful rsc of any rscDfn, so no need to notify scheduled shipping
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

    private boolean couldTakeover(Resource rscRef, AutoHelperContext ctxRef) throws AccessDeniedException
    {
        boolean couldTakeover = false;
        final AccessContext peerAccCtx = peerCtx.get();

        if (rscRef.getStateFlags().isSet(peerAccCtx, Resource.Flags.DRBD_DISKLESS))
        {
            final ResourceDefinition rscDfn = ctxRef.rscDfn;
            final Predicate<StorPool> isEligibleTieBreakerStorPool = getEligibleTieBreakerStorPoolPredicate(rscDfn);
            couldTakeover = LayerVlmUtils.getStorPools(rscRef, peerAccCtx)
                .stream()
                .allMatch(isEligibleTieBreakerStorPool);
        }
        else
        {
            // will be null if rscRef already violates some RG settings. This rsc should not be taken over, but rather
            // find a new node for the tiebreaker
            @Nullable StorPool storPoolForTieBreaker = getStorPoolForTieBreaker(
                ctxRef,
                Collections.singleton(rscRef.getNode())
            );
            couldTakeover = storPoolForTieBreaker != null;
        }
        return couldTakeover;
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
            flags.disableFlags(accCtx, Resource.Flags.DRBD_DELETE, Resource.Flags.DELETE);

            Iterator<Volume> vlmsIt = rsc.iterateVolumes();
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();
                vlm.getFlags().disableFlags(accCtx, Volume.Flags.DRBD_DELETE, Volume.Flags.DELETE);
            }

            // just to be sure
            ResourceDataUtils.recalculateVolatileRscData(layerDataHelper, rsc);

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
                        rsc.getResourceDefinition().getName().displayValue,
                        null,
                        null,
                        null,
                        true,
                        null,
                        true
                    )
                );

                ctx.preventUpdateSatellitesForResourceDelete = true;
                ctx.requiresUpdateFlux = false; // resourceToggleDisk already performs stltUpdate
            }
            ctx.responses.addEntries(
                ApiCallRcImpl.singleApiCallRc(
                    ApiConsts.INFO_TIE_BREAKER_TAKEOVER,
                    "The given resource will not be deleted but will be taken over as a " +
                        "linstor managed tiebreaker resource."
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

    private void enableAutoTieBreaker(AutoHelperContext ctxRef) throws DatabaseException
    {
        try
        {
            ctxRef.rscDfn.getProps(peerCtx.get())
                .setProp(KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER, VAL_TRUE, NAMESPC_DRBD_OPTIONS);
            ctxRef.responses.add(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_SET,
                    "Autotiebreaker automatically enabled due to --keep-tiebreaker"
                )
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "enabling auto-tiebreaker feature " + getRscDfnDescriptionInline(ctxRef.rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
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
        long diskfulDrbdCount = 0;
        long disklessDrbdCount = 0;

        AccessContext peerAccCtx = peerCtx.get();

        if (CtrlRscAutoQuorumHelper.isAutoQuorumEnabled(getPrioProps(rscDfn)))
        {
            Predicate<StorPool> isEligibleTieBreakerStorPool = getEligibleTieBreakerStorPoolPredicate(rscDfn);

            Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx);
            while (rscIt.hasNext())
            {
                Resource rsc = rscIt.next();

                StateFlags<Resource.Flags> rscFlags = rsc.getStateFlags();

                if (isDrbdResource(peerAccCtx, rsc))
                {
                    if (rscFlags.isSet(peerAccCtx, Resource.Flags.DRBD_DISKLESS))
                    {
                        boolean eligibleStoragePool = LayerVlmUtils.getStorPools(rsc, peerAccCtx)
                            .stream()
                            .allMatch(isEligibleTieBreakerStorPool);
                        if (eligibleStoragePool && !rscFlags.isSet(peerAccCtx, Resource.Flags.TIE_BREAKER))
                        {
                            // We only count non-tie-breaker diskless resource, which are also "eligible"
                            // to be tiebreakers, i.e. the autoplacer could have chosen this node, assuming
                            // it only saw the diskful resources.
                            disklessDrbdCount++;
                        }
                    }
                    else
                    {
                        diskfulDrbdCount++;
                    }
                }
            }
        }
        return diskfulDrbdCount >= 2 && diskfulDrbdCount % 2 == 0 && disklessDrbdCount == 0;
    }

    private Predicate<StorPool> getEligibleTieBreakerStorPoolPredicate(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        List<Node> alreadyDeployedDiskfulNodes = new ArrayList<>();

        AccessContext peerAccCtx = peerCtx.get();
        Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();

            if (!rsc.isDiskless(peerAccCtx))
            {
                alreadyDeployedDiskfulNodes.add(rsc.getNode());
            }
        }

        AutoSelectFilterPojo mergedAutoSelectFilter = AutoSelectFilterPojo.merge(
            new AutoSelectFilterBuilder()
                .setPlaceCount(0)
                .setAdditionalPlaceCount(1)
                .setDoNotPlaceWithRscList(Collections.singletonList(rscDfn.getName().displayValue))
                .setLayerStackList(Collections.singletonList(DeviceLayerKind.DRBD))
                .setDisklessType(Resource.Flags.DRBD_DISKLESS.name())
                .build(),
            rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData()
        );

        SelectionManager selectionManager = new SelectionManager(
            peerAccCtx,
            errorReporter,
            mergedAutoSelectFilter,
            alreadyDeployedDiskfulNodes,
            alreadyDeployedDiskfulNodes.size(),
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            null,
            false // not that it matters for tiebreaker selection
        );

        return storPool -> {
            boolean isAllowed = false;
            try
            {
                isAllowed = selectionManager.isAllowed(storPool);
            }
            catch (AccessDeniedException e)
            {
                // Ignore -> not allowed
            }
            return isAllowed;
        };
    }

    private boolean isDrbdResource(AccessContext peerAccCtx, Resource rsc) throws AccessDeniedException
    {
        final DrbdResourceResult result = ResourceDataUtils.isDrbdResource(rsc, peerAccCtx);
        final StateFlags<Resource.Flags> rscFlags = rsc.getStateFlags();

        return result != DrbdResourceResult.NO_DRBD &&
            rscFlags.isUnset(peerAccCtx, Resource.Flags.DELETE) &&
            rscFlags.isUnset(peerAccCtx, Resource.Flags.DRBD_DELETE) &&
            rscFlags.isUnset(peerAccCtx, Resource.Flags.INACTIVE);
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

    private StorPool getStorPoolForTieBreaker(AutoHelperContext ctx, @Nullable Collection<Node> nodesToChooseFromRef)
    {
        StorPool storPool = null;

        try
        {
            AccessContext peerAccCtx = peerCtx.get();

            List<String> filterNodeNamesList = new ArrayList<>();
            if (nodesToChooseFromRef != null)
            {
                for (Node node : nodesToChooseFromRef)
                {
                    filterNodeNamesList.add(node.getName().displayValue);
                }

            }

            while (storPool == null)
            {
                AutoSelectFilterApi apiData = ctx.rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData();
                @Nullable Map<String, Integer> xReplicasOnDifferentMap = apiData.getXReplicasOnDifferentMap();
                @Nullable Map<String, Integer> xReplOnDiffCopyOrNull;
                if (xReplicasOnDifferentMap != null)
                {
                    xReplOnDiffCopyOrNull = new HashMap<>(xReplicasOnDifferentMap);
                    // in order to force the tiebreaker to be put on a new datacenter, we want to (temporarily) override
                    // all values of the xReplicasOnDifferentMap with 1
                    for (Entry<String, Integer> entry : xReplOnDiffCopyOrNull.entrySet())
                    {
                        entry.setValue(1);
                    }
                }
                else
                {
                    xReplOnDiffCopyOrNull = null;
                }
                AutoSelectFilterPojo mergedAutoSelectFilterPojo = AutoSelectFilterPojo.merge(
                    new AutoSelectFilterBuilder()
                        .setPlaceCount(0)
                        .setAdditionalPlaceCount(1)
                        .setNodeNameList(filterNodeNamesList)
                        .setSkipAlreadyPlacedOnNodeNamesCheck(
                            filterNodeNamesList.isEmpty() ? null : filterNodeNamesList
                        )
                        .setDoNotPlaceWithRscList(Collections.singletonList(ctx.rscDfn.getName().displayValue))
                        .setLayerStackList(Collections.singletonList(DeviceLayerKind.DRBD))
                        .setDisklessType(Resource.Flags.DRBD_DISKLESS.name())
                        .setXReplicasOnDifferentMap(xReplOnDiffCopyOrNull)
                        .build(),
                    ctx.rscDfn.getResourceGroup().getAutoPlaceConfig().getApiData(),
                    ctx.selectFilter
                );
                /*
                 * There are two possibilities:
                 * 1) we just autoplaced at least 1 diskful resource so we reached the 2 diskful + 0 diskless condition
                 * that we now place a tiebreaker. That means that if the ctx.selectFilter.getStorPoolNamesList is not
                 * null it will contain the name(s) of disk*ful* storage pool(s). We need to ignore those as otherwise
                 * we will not be able to find a diskless storage pool for our tiebreaker
                 * 2) we just autoplaced at least 1 diskless resource. In this case, the condition of 2 diskful and
                 * *0* diskless cannot be fulfilled, so we cannot be here :)
                 *
                 * That means, it should be always safe to also ignore the storPoolNameList of the ctx.selectFilter as
                 * well as from the resourcegroup (i.e. the merged result)
                 */
                // TODO: This is no longer true...
                mergedAutoSelectFilterPojo.setStorPoolNameList(null);

                Set<StorPool> autoplaceResult = autoplacer.autoPlace(
                    mergedAutoSelectFilterPojo,
                    ctx.rscDfn,
                    0 // doesn't matter as we are diskless
                );

                if (autoplaceResult != null && !autoplaceResult.isEmpty())
                {
                    storPool = autoplaceResult.iterator().next();
                    if (!supportsTieBreaker(peerAccCtx, storPool.getNode()))
                    {
                        /*
                         * autoplacer only checks if DRBD >= 9 is supported, but we need >= 9.0.19
                         */
                        filterNodeNamesList.remove(storPool.getNode().getName().displayValue);
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
            .getExtToolInfo(ExtTools.DRBD9_KERNEL)
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

    private boolean isSomeFlagSet(Resource rsc, Resource.Flags... flags)
    {
        boolean isFlagSet;
        try
        {
            isFlagSet = rsc.getStateFlags().isSomeSet(peerCtx.get(), flags);
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

    private boolean isSomeFlagSet(@Nonnull Node node, @Nonnull Node.Flags... flags)
    {
        boolean isFlagSet;
        try
        {
            isFlagSet = node.getFlags().isSomeSet(peerCtx.get(), flags);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "checking flag state of " + node,
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return isFlagSet;
    }

    /**
     * Sets the tiebreaker flag in a transaction and commits the transaction.
     * Does NOT update satellites
     *
     * @param tiebreaker
     * @return
     */
    public Flux<ApiCallRc> setTiebreakerFlag(Resource tiebreaker)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeModifyOperation(),
            tiebreaker.getNode().getName().getDisplayName(),
            tiebreaker.getResourceDefinition().getName().getDisplayName()
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
