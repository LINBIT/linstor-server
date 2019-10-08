package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.apicallhandler.ScopeRunner;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiOperation;
import com.linbit.linstor.core.apicallhandler.response.ResponseContext;
import com.linbit.linstor.core.apicallhandler.response.ResponseConverter;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.CtrlLayerDataHelper;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscDfnData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import reactor.core.publisher.Flux;

@Singleton
public class CtrlRscAutoTieBreakerHelper
{
    private final SystemConfRepository systemConfRepository;
    private final CtrlLayerDataHelper layerDataHelper;
    private final NodeRepository nodeRepo;
    private final CtrlRscCrtApiHelper rscCrtApiHelper;
    private final Provider<AccessContext> peerCtx;
    private final ScopeRunner scopeRunner;
    private final ReadWriteLock nodesMapLock;
    private final ReadWriteLock rscDfnMapLock;
    private final ResponseConverter responseConverter;
    private final CtrlTransactionHelper ctrlTransactionHelper;

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
        CtrlLayerDataHelper layerDataHelperRef,
        @PeerContext Provider<AccessContext> peerCtxRef,
        @Named(CoreModule.NODES_MAP_LOCK) ReadWriteLock nodesMapLockRef,
        @Named(CoreModule.RSC_DFN_MAP_LOCK) ReadWriteLock rscDfnMapLockRef,
        CtrlRscCrtApiHelper rscCrtApiHelperRef,
        ResponseConverter responseConverterRef,
        CtrlTransactionHelper ctrlTransactionHelperRef
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
    }

    public AutoTiebreakerResult manage(
        ApiCallRcImpl apiCallRcImpl,
        ResourceDefinition rscDfn,
        Set<Resource> candidatesToTakeOver
    )
    {
        AutoTiebreakerResult result = new AutoTiebreakerResult();
        try
        {
            if (isAutoTieBreakerEnabled(rscDfn))
            {
                Resource tieBreaker = getTieBreaker(rscDfn);
                if (shouldTieBreakerExist(rscDfn))
                {
                    if (tieBreaker == null)
                    {
                        Resource diskfulCandidate = null;
                        Resource takeover = null;
                        for (Resource rsc : candidatesToTakeOver)
                        {
                            if (isFlagSet(rsc, Resource.Flags.DELETE))
                            {
                                if (isFlagSet(rsc, Resource.Flags.DISKLESS))
                                {
                                    takeover = rsc;
                                    break;
                                }
                                else
                                {
                                    diskfulCandidate = rsc;
                                    // keep looking for diskless resource
                                }
                            }
                        }
                        if (takeover != null)
                        {
                            takeover(takeover, true, result, apiCallRcImpl);
                        }
                        else
                        if (diskfulCandidate != null)
                        {
                            takeover(diskfulCandidate, false, result, apiCallRcImpl);
                        }
                        else
                        {
                            Node node = getNodeForTieBreaker(rscDfn);
                            if (node == null)
                            {
                                apiCallRcImpl.addEntries(
                                    ApiCallRcImpl.singleApiCallRc(
                                        ApiConsts.WARN_NOT_ENOUGH_NODES_FOR_TIE_BREAKER,
                                        "Could not find suitable node to automatically create a tie breaking resource."
                                    )
                                );
                            }
                            else
                            {
                                tieBreaker = rscCrtApiHelper.createResourceDb(
                                    node.getName().displayValue,
                                    rscDfn.getName().displayValue,
                                    Resource.Flags.TIE_BREAKER.flagValue,
                                    Collections.emptyMap(),
                                    Collections.emptyList(),
                                    null,
                                    Collections.emptyMap(),
                                    Collections.emptyList()
                                ).extractApiCallRc(apiCallRcImpl);

                                apiCallRcImpl.addEntries(
                                    ApiCallRcImpl.singleApiCallRc(
                                        ApiConsts.INFO_TIE_BREAKER_CREATED,
                                        "Tie breaker resource created on " + node.getName().displayValue
                                    )
                                );

                                result.created = tieBreaker;
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
                            apiCallRcImpl.addEntries(
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
                        apiCallRcImpl.addEntries(
                            ApiCallRcImpl.singleApiCallRc(
                                ApiConsts.INFO_TIE_BREAKER_DELETING,
                                "Tie breaker marked for deletion"
                            )
                        );
                        result.deleting = tieBreaker;
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "managing auto-quorum feature " + getRscDfnDescriptionInline(rscDfn),
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
        return result;
    }

    private void takeover(Resource rsc, boolean diskless, AutoTiebreakerResult result, ApiCallRcImpl apiCallRcImpl)
    {
        StateFlags<Flags> flags = rsc.getStateFlags();
        try
        {
            AccessContext accCtx = peerCtx.get();
            flags.disableFlags(accCtx, Resource.Flags.DELETE);

            Iterator<Volume> vlmsIt = rsc.iterateVolumes();
            while (vlmsIt.hasNext())
            {
                Volume vlm = vlmsIt.next();
                vlm.getFlags().disableFlags(accCtx, Volume.Flags.DELETE);
            }

            if (diskless)
            {
                result.takeoverDiskless = rsc;
            }
            else
            {
                result.takeoverDiskful = rsc;
            }
            apiCallRcImpl.addEntries(
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
                "checking auto-quorum feature " + getRscDfnDescriptionInline(rscDfn),
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
            if (((DrbdRscDfnData) rscDfnData).getPeerSlots() <= currentCount)
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
                    if (rscFlags.isSet(peerAccCtx, Resource.Flags.DISKLESS))
                    {
                        disklessDrbdCount++;
                    }
                    else
                    {
                        diskfulDrbdCount++;
                    }
                }

                RscLayerObject layerData = rsc.getLayerData(peerAccCtx);
                Set<RscLayerObject> drbdDataSet = LayerRscUtils.getRscDataByProvider(layerData, DeviceLayerKind.DRBD);
                for (RscLayerObject rlo : drbdDataSet)
                {
                    if (((DrbdRscData) rlo).getPeerSlots() <= currentCount)
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

    private Node getNodeForTieBreaker(ResourceDefinition rscDfnRef)
    {
        Node tieBreakerNode = null;
        try
        {
            AccessContext peerAccCtx = peerCtx.get();
            NodesMap mapForView = nodeRepo.getMapForView(peerAccCtx);
            for (Node node : mapForView.values())
            {
                if (
                    node.getObjProt().queryAccess(peerAccCtx).hasAccess(AccessType.USE) &&
                    rscDfnRef.getResource(peerAccCtx, node.getName()) == null &&
                    supportsTieBreaker(peerAccCtx, node)
                )
                {
                    tieBreakerNode = node;
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
        return tieBreakerNode;
    }

    private boolean supportsTieBreaker(AccessContext peerAccCtx, Node node) throws AccessDeniedException
    {
        return node.getPeer(peerAccCtx)
            .getExtToolsManager()
            .getExtToolInfo(ExtTools.DRBD9)
            .hasVersionOrHigher(9, 0, 19);
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

    public Flux<ApiCallRc> setTiebreakerFlag(Resource tiebreaker)
    {
        ResponseContext context = CtrlRscApiCallHandler.makeRscContext(
            ApiOperation.makeModifyOperation(),
            tiebreaker.getAssignedNode().getName().getDisplayName(),
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
