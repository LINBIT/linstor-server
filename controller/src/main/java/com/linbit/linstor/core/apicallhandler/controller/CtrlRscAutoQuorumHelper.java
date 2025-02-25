package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
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
import com.linbit.linstor.layer.resource.CtrlRscLayerDataFactory;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import static com.linbit.linstor.api.ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS;
import static com.linbit.linstor.core.apicallhandler.controller.CtrlRscDfnApiCallHandler.getRscDfnDescriptionInline;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

@Singleton
public class CtrlRscAutoQuorumHelper implements CtrlRscAutoHelper.AutoHelper
{
    private static final String PROP_KEY_QUORUM = "quorum";
    private static final String PROP_VAL_QUORUM_MAJORITY = "majority";
    private static final String PROP_VAL_QUORUM_OFF = "off";

    private static final String PROP_KEY_ON_NO_QUORUM = "on-no-quorum";

    private final SystemConfRepository systemConfRepository;
    private final CtrlRscLayerDataFactory layerDataHelper;
    private final Provider<AccessContext> peerCtx;

    @Inject
    public CtrlRscAutoQuorumHelper(
        SystemConfRepository systemConfRepositoryRef,
        CtrlRscLayerDataFactory layerDataHelperRef,
        @PeerContext Provider<AccessContext> peerCtxRef
    )
    {
        systemConfRepository = systemConfRepositoryRef;
        layerDataHelper = layerDataHelperRef;
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
        String autoQuorum = getAutoQuorum(ctx.rscDfn);
        if (autoQuorum != null && !autoQuorum.equals(ApiConsts.VAL_DRBD_AUTO_QUORUM_DISABLED))
        {
            try
            {
                Set<Node> involvedNodesWithoutQuorumSupport = getNodesNotSupportingQuroum(peerCtx.get(), ctx.rscDfn);
                Props props = ctx.rscDfn.getProps(peerCtx.get());

                if (isQuorumFeasible(ctx.rscDfn))
                {
                    if (involvedNodesWithoutQuorumSupport.isEmpty())
                    {
                        activateQuorum(ctx.responses, autoQuorum, props);
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
                                involvedNodesWithoutQuorumSupport.toString(),
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
    }

    private void activateQuorum(ApiCallRcImpl apiCallRcImpl, String autoQuorum, Props props)
        throws InvalidValueException, AccessDeniedException, DatabaseException
    {
        String oldQuorum = props.setProp(
            PROP_KEY_QUORUM,
            PROP_VAL_QUORUM_MAJORITY,
            NAMESPC_DRBD_RESOURCE_OPTIONS
        );
        String oldOnNoQuorum = props.setProp(
            PROP_KEY_ON_NO_QUORUM,
            autoQuorum,
            NAMESPC_DRBD_RESOURCE_OPTIONS
        );

        if (!PROP_VAL_QUORUM_MAJORITY.equals(oldQuorum))
        {
            apiCallRcImpl.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_SET,
                    String.format(
                        "Resource-definition property '%s/%s' updated from %s to '%s' by auto-quorum",
                        NAMESPC_DRBD_RESOURCE_OPTIONS,
                        PROP_KEY_QUORUM,
                        (oldQuorum == null ? "undefined" : "'" + oldQuorum + "'"),
                        PROP_VAL_QUORUM_MAJORITY
                    )
                )
            );
        }
        if (!autoQuorum.equals(oldOnNoQuorum))
        {
            apiCallRcImpl.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_SET,
                    String.format(
                        "Resource-definition property '%s/%s' updated from %s to '%s' by auto-quorum",
                        NAMESPC_DRBD_RESOURCE_OPTIONS,
                        PROP_KEY_ON_NO_QUORUM,
                        (oldQuorum == null ? "undefined" : "'" + oldQuorum + "'"),
                        autoQuorum
                    )
                )
            );
        }
    }

    private void deactivateQuorum(ApiCallRcImpl apiCallRcImpl, Props props, String reason)
        throws InvalidKeyException, AccessDeniedException, DatabaseException, InvalidValueException
    {
        String oldQuorum = props.setProp(PROP_KEY_QUORUM, PROP_VAL_QUORUM_OFF, NAMESPC_DRBD_RESOURCE_OPTIONS);
        String oldOnNoQuorum = props.removeProp(PROP_KEY_ON_NO_QUORUM, NAMESPC_DRBD_RESOURCE_OPTIONS);

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
        if (oldOnNoQuorum != null)
        {
            apiCallRcImpl.addEntry(
                ApiCallRcImpl.simpleEntry(
                    ApiConsts.INFO_PROP_REMOVED,
                    String.format(
                        "Resource-definition property '%s/%s' was removed%s",
                        NAMESPC_DRBD_RESOURCE_OPTIONS,
                        PROP_KEY_ON_NO_QUORUM,
                        reason
                    )
                )
            );
        }
    }

    public String getAutoQuorum(ResourceDefinition rscDfn)
    {
        String autoQuorum;
        try
        {
            autoQuorum = getAutoQuorumProp(getPrioProps(rscDfn, peerCtx.get()));
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
        return autoQuorum;
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

    public static boolean isAutoQuorumEnabled(PriorityProps prioProps)
    {
        String autoQuorum = getAutoQuorumProp(prioProps);
        return autoQuorum != null && !autoQuorum.equals(ApiConsts.VAL_DRBD_AUTO_QUORUM_DISABLED);
    }

    private static String getAutoQuorumProp(PriorityProps prioProps)
    {
        return prioProps.getProp(ApiConsts.KEY_DRBD_AUTO_QUORUM, ApiConsts.NAMESPC_DRBD_OPTIONS);
    }

    public static boolean isQuorumSupportedByAllNodes(AccessContext accCtx, ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        return getNodesNotSupportingQuroum(accCtx, rscDfn).isEmpty();
    }

    public static Set<Node> getNodesNotSupportingQuroum(AccessContext accCtx, ResourceDefinition rscDfn)
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
        return node.getPeer(accCtx)
            .getExtToolsManager()
            .getExtToolInfo(ExtTools.DRBD9_KERNEL)
            .hasVersionOrHigher(new ExtToolsInfo.Version(9, 0, 18));
    }
}
