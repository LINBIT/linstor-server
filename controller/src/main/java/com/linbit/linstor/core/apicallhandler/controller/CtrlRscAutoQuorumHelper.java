package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils;
import com.linbit.linstor.core.apicallhandler.controller.utils.ResourceDataUtils.DrbdResourceResult;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.utils.Pair;

import static com.linbit.linstor.api.ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * The current implementation of Linstor auto-quorum works currently directly on the DrbdOptions/Resource/quorum
 * property.
 * Linstor tracks if the property was set by the user or by Linstor itself via (Internal/Drbd/QuorumSetBy).
 * If it was set by the user, Linstor doesn't automate any of the quorum settings and just takes the property settings.
 * If it isn't set or set by Linstor, all the automagic comes into play, the quorum option will be set on RscDfn level
 * by Linstor. It should also be deleted (@see CtrlRscAutoQuorumHelper#removeQuorumPropIfSetByLinstor())
 * by Linstor if it is explicitly set on a higher level (C, RG).
 */
@Singleton
class CtrlRscAutoQuorumHelper implements CtrlRscAutoHelper.AutoHelper
{
    private static final String PROP_KEY_QUORUM = "quorum";
    private static final String PROP_VAL_QUORUM_MAJORITY = "majority";
    private static final String PROP_VAL_QUORUM_OFF = "off";

    private final SystemConfRepository systemConfRepository;
    private final Provider<AccessContext> peerCtx;

    @Inject
    CtrlRscAutoQuorumHelper(
        SystemConfRepository systemConfRepositoryRef,
        @PeerContext Provider<AccessContext> peerCtxRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        peerCtx = peerCtxRef;
    }

    @Override
    public CtrlRscAutoHelper.AutoHelperType getType()
    {
        return CtrlRscAutoHelper.AutoHelperType.AutoQuorum;
    }

    @Override
    public void manage(AutoHelperContext ctx)
    {
        if (isAutoQuorumEnabled(ctx.rscDfn))
        {
            try
            {
                Set<Node> involvedNodesWithoutQuorumSupport = getNodesNotSupportingQuorum(peerCtx.get(), ctx.rscDfn);
                Props props = ctx.rscDfn.getProps(peerCtx.get());

                if (isQuorumFeasible(ctx.rscDfn))
                {
                    if (involvedNodesWithoutQuorumSupport.isEmpty())
                    {
                        activateQuorum(ctx.responses, props);
                    }
                    else
                    {
                        boolean singular = involvedNodesWithoutQuorumSupport.size() == 1;
                        deactivateQuorum(
                            ctx.responses,
                            props,
                            String.format(
                                " as the node%s %s do%s not support DRBD quorum",
                                singular ? "" : "s",
                                involvedNodesWithoutQuorumSupport,
                                singular ? "es" : ""
                            )
                        );
                    }
                }
                else
                {
                    deactivateQuorum(ctx.responses, props, " as there are not enough resources for quorum");
                }
            }
            catch (AccessDeniedException accDeniedExc)
            {
                throw new ApiAccessDeniedException(
                    accDeniedExc,
                    "checking auto-quorum feature " + getRscDfnDescriptionInline(ctx.rscDfn),
                    ApiConsts.FAIL_ACC_DENIED_RSC_DFN
                );
            }
            catch (InvalidKeyException | InvalidValueException exc)
            {
                throw new ImplementationError(exc);
            }
            catch (DatabaseException exc)
            {
                throw new ApiDatabaseException(exc);
            }
        }
        else
        {
            // set depending prio prop
            setQuorumFromUser(ctx.responses, ctx.rscDfn);
        }
    }

    private void setQuorumFromUser(ApiCallRcImpl apiCallRc, ResourceDefinition rscDfn)
    {
        try
        {
            var quorumSetBy = getQuorumSetByProp(getPrioProps(rscDfn, peerCtx.get()));
            if (!InternalApiConsts.SET_BY_VALUE_LINSTOR.equalsIgnoreCase(quorumSetBy.objA))
            {
                Props rscDfnProps = rscDfn.getProps(peerCtx.get());
                String oldQuorumProp = rscDfnProps.getProp(PROP_KEY_QUORUM, NAMESPC_DRBD_RESOURCE_OPTIONS);
                String quorumProp = quorumSetBy.objB.getProp(PROP_KEY_QUORUM, NAMESPC_DRBD_RESOURCE_OPTIONS);
                quorumProp = quorumProp == null ? PROP_VAL_QUORUM_MAJORITY : quorumProp;
                if (!quorumProp.equalsIgnoreCase(oldQuorumProp))
                {
                    rscDfn.getProps(peerCtx.get()).setProp(PROP_KEY_QUORUM, quorumProp, NAMESPC_DRBD_RESOURCE_OPTIONS);

                    apiCallRc.addEntry(
                        ApiCallRcImpl.simpleEntry(
                            ApiConsts.INFO_PROP_SET,
                            String.format(
                                "Resource-definition property '%s/%s' updated from %s to '%s' by User",
                                NAMESPC_DRBD_RESOURCE_OPTIONS,
                                PROP_KEY_QUORUM,
                                (oldQuorumProp == null ? "undefined" : "'" + oldQuorumProp + "'"),
                                quorumProp
                            )
                        )
                    );
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "checking auto-quorum feature " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (InvalidKeyException | InvalidValueException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }

    private void activateQuorum(ApiCallRcImpl apiCallRcImpl, Props props)
        throws InvalidValueException, AccessDeniedException, DatabaseException
    {
        String oldQuorum = props.setProp(
            PROP_KEY_QUORUM,
            PROP_VAL_QUORUM_MAJORITY,
            NAMESPC_DRBD_RESOURCE_OPTIONS
        );

        if (!PROP_VAL_QUORUM_MAJORITY.equals(oldQuorum))
        {
            apiCallRcImpl.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_SET,
                    String.format(
                        "Resource-definition property '%s/%s' updated from %s to '%s' by AutoQuorum",
                        NAMESPC_DRBD_RESOURCE_OPTIONS,
                        PROP_KEY_QUORUM,
                        (oldQuorum == null ? "undefined" : "'" + oldQuorum + "'"),
                        PROP_VAL_QUORUM_MAJORITY
                    )
                )
            );
        }
    }

    private void deactivateQuorum(ApiCallRcImpl apiCallRcImpl, Props props, String reason)
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        String oldQuorum = props.setProp(PROP_KEY_QUORUM, PROP_VAL_QUORUM_OFF, NAMESPC_DRBD_RESOURCE_OPTIONS);

        if (oldQuorum != null && !oldQuorum.equals(PROP_VAL_QUORUM_OFF))
        {
            apiCallRcImpl.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_REMOVED,
                    String.format(
                        "Resource-definition property '%s/%s' was removed%s",
                        NAMESPC_DRBD_RESOURCE_OPTIONS,
                        PROP_KEY_QUORUM,
                        reason
                    )
                )
            );
        }
    }

    public boolean isAutoQuorumEnabled(ResourceDefinition rscDfn)
    {
        String quorumSetBy;
        try
        {
            quorumSetBy = getQuorumSetByProp(getPrioProps(rscDfn, peerCtx.get())).objA;
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
        return InternalApiConsts.SET_BY_VALUE_LINSTOR.equalsIgnoreCase(quorumSetBy);
    }

    private PriorityProps getPrioProps(ResourceDefinition rscDfn, AccessContext peerAccCtx)
        throws AccessDeniedException
    {
        return new PriorityProps(
            rscDfn.getProps(peerAccCtx),
            rscDfn.getResourceGroup().getProps(peerAccCtx),
            systemConfRepository.getCtrlConfForView(peerAccCtx)
        );
    }

    private boolean isQuorumFeasible(ResourceDefinition rscDfn) throws AccessDeniedException
    {
        int diskfulDrbdCount = 0;
        int disklessDrbdCount = 0;
        AccessContext peerAccCtx = peerCtx.get();
        Iterator<Resource> rscIt = rscDfn.iterateResource(peerAccCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            StateFlags<Flags> rscFlags = rsc.getStateFlags();

            DrbdResourceResult result = ResourceDataUtils.isDrbdResource(rsc, peerAccCtx);

            if (result != DrbdResourceResult.NO_DRBD &&
                rscFlags.isUnset(peerAccCtx, Resource.Flags.DELETE) &&
                rscFlags.isUnset(peerAccCtx, Resource.Flags.INACTIVE)
            )
            {
                if (rscFlags.isSet(peerAccCtx, Resource.Flags.DRBD_DISKLESS))
                {
                    disklessDrbdCount++;
                }
                else
                {
                    diskfulDrbdCount++;
                }
            }
        }
        return (diskfulDrbdCount == 2 && disklessDrbdCount >= 1) ||
            diskfulDrbdCount >= 3;
    }

    /**
     * Checks if auto-quorum is enabled by Linstor.
     * @param prioProps PrioProps(RD, RG, C)
     * @return false if 'DrbdOptions/Resource/quorum' was directly set the user on any prioProp(RD, RG, C) level.
     */
    public static boolean isAutoQuorumEnabled(PriorityProps prioProps)
    {
        var quorumSetBy = getQuorumSetByProp(prioProps);
        return InternalApiConsts.SET_BY_VALUE_LINSTOR.equalsIgnoreCase(quorumSetBy.objA);
    }

    /**
     * Checks if the quorum option is set to `majority` on any PrioProp(RD, RG, C) level.
     * @param prioProps PrioProps(RD, RG, C)
     * @return true if DrbdOptions/Resource/quorum has majority on any prioProp level.
     */
    public static boolean isQuorumEnabled(PriorityProps prioProps)
    {
        String quorumSetting = prioProps.getProp(
            InternalApiConsts.KEY_DRBD_QUORUM, ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS, PROP_VAL_QUORUM_MAJORITY);
        return quorumSetting.equalsIgnoreCase(PROP_VAL_QUORUM_MAJORITY);
    }

    private static Pair<String, ReadOnlyProps> getQuorumSetByProp(PriorityProps prioProps)
    {
        return prioProps.getPropAndContainer(
            ApiConsts.KEY_QUORUM_SET_BY, ApiConsts.NAMESPC_INTERNAL_DRBD, InternalApiConsts.SET_BY_VALUE_LINSTOR);
    }

    public static boolean isQuorumSupportedByAllNodes(AccessContext accCtx, ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        return getNodesNotSupportingQuorum(accCtx, rscDfn).isEmpty();
    }

    public static Set<Node> getNodesNotSupportingQuorum(AccessContext accCtx, ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        Set<Node> ret = new TreeSet<>();
        Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
        while (rscIt.hasNext())
        {
            Node node = rscIt.next().getNode();
            if (!supportsQuorum(accCtx, node))
            {
                ret.add(node);
            }
        }
        return ret;
    }

    public static boolean supportsQuorum(AccessContext accCtx, Node node) throws AccessDeniedException
    {
        ExtToolsInfo drbdInfo = node.getPeer(accCtx)
            .getExtToolsManager()
            .getExtToolInfo(ExtTools.DRBD9_KERNEL);
        return drbdInfo != null && drbdInfo
            .hasVersionOrHigher(new ExtToolsInfo.Version(9, 0, 18));
    }

    /**
     * This method should be called if a higher level objects (Controller, RG) `quorum` property was modified.
     * It will delete the rscDfn quorum property if it was auto set by Linstor, to guarantee that the higher level
     * prio prop will be taken into account.
     * @param rscDfn RD to check and delete the quorum prop if necessary
     * @param accCtx access context
     */
    public static void removeQuorumPropIfSetByLinstor(ResourceDefinition rscDfn, AccessContext accCtx)
    {
        try
        {
            Props props = rscDfn.getProps(accCtx);
            String quorumSetByLinstor = props.getPropWithDefault(
                ApiConsts.KEY_QUORUM_SET_BY, ApiConsts.NAMESPC_INTERNAL_DRBD, InternalApiConsts.SET_BY_VALUE_LINSTOR);
            if (quorumSetByLinstor.equalsIgnoreCase(InternalApiConsts.SET_BY_VALUE_LINSTOR))
            {
                props.removeProp(InternalApiConsts.KEY_DRBD_QUORUM, ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS);
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "cleanup Linstor set quorum property " + getRscDfnDescriptionInline(rscDfn),
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        catch (DatabaseException exc)
        {
            throw new ApiDatabaseException(exc);
        }
    }
}
