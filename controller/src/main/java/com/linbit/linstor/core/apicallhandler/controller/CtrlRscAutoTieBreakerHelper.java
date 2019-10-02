package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.CoreModule.NodesMap;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.NodeRepository;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.CtrlLayerDataHelper;
import com.linbit.linstor.propscon.InvalidKeyException;
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

import static com.linbit.linstor.api.ApiConsts.KEY_DRBD_AUTO_ADD_QUORUM_TIEBREAKER;
import static com.linbit.linstor.api.ApiConsts.NAMESPC_DRBD_OPTIONS;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

@Singleton
public class CtrlRscAutoTieBreakerHelper
{
    private final SystemConfRepository systemConfRepository;
    private final CtrlLayerDataHelper layerDataHelper;
    private final NodeRepository nodeRepo;
    private final CtrlRscCrtApiHelper rscCrtApiHelper;
    private final Provider<AccessContext> peerCtx;

    @Inject
    public CtrlRscAutoTieBreakerHelper(
        SystemConfRepository systemConfRepositoryRef,
        NodeRepository nodeRepoRef,
        CtrlLayerDataHelper layerDataHelperRef,
        @PeerContext Provider<AccessContext> peerCtxRef,
        CtrlRscCrtApiHelper rscCrtApiHelperRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        nodeRepo = nodeRepoRef;
        layerDataHelper = layerDataHelperRef;
        peerCtx = peerCtxRef;
        rscCrtApiHelper = rscCrtApiHelperRef;
    }

    /**
     * <pre>
     * if auto-tie-breaker is enabled
     * {
     *     if a tie breaker is required
     *     {
     *          if a tie breaker exists
     *          {
     *              (noop)
     *              return null
     *          }
     *          else
     *          {
     *              if there is an available node supporting DRBD tie-breaker
     *              {
     *                  add message to apiCallRc
     *                  create tie breaker resource in db (no satellite-update) on that node
     *                  return created tie breaker resource
     *              }
     *              else
     *              {
     *                  add message to apiCallRc
     *                  return null
     *              }
     *          }
     *      }
     *      else
     *      {
     *          if a tie breaker exists
     *          {
     *              add message to apiCallRc
     *              mark tie breaker as deleted (no satellite-update, no db-delete, only DELETE flag update)
     *              return tie breaker resource
     *          }
     *          else
     *          {
     *              return null
     *          }
     *      }
     * }
     * else
     * {
     *      noop
     *      return null
     * }
     * </pre>
     *
     * @param apiCallRcImpl
     * @param rscDfn
     */
    public Resource manage(ApiCallRcImpl apiCallRcImpl, ResourceDefinition rscDfn)
    {
        Resource ret = null;
        try
        {
            if (isAutoTieBreakerEnabled(rscDfn))
            {
                Resource tieBreaker = getTieBreaker(rscDfn);
                if (shouldTieBreakerExist(rscDfn))
                {
                    if (tieBreaker == null)
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

                            ret = tieBreaker;
                        }
                    }
                    // else: noop
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
                        ret = tieBreaker;
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
        return ret;
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
}
