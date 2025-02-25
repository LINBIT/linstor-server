package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperContext;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Iterator;

@Singleton
public class CtrlRscAutoDrbdProxyHelper implements AutoHelper
{
    private final Provider<AccessContext> peerCtxProvider;
    private final CtrlDrbdProxyHelper drbdProxyHelper;
    private final SystemConfRepository systemConfRepository;

    @Inject
    public CtrlRscAutoDrbdProxyHelper(
        @PeerContext Provider<AccessContext> peerCtxProviderRef,
        CtrlDrbdProxyHelper drbdProxyHelperRef,
        SystemConfRepository systemConfRepositoryRef
    )
    {
        peerCtxProvider = peerCtxProviderRef;
        drbdProxyHelper = drbdProxyHelperRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    @Override
    public CtrlRscAutoHelper.AutoHelperType getType()
    {
        return CtrlRscAutoHelper.AutoHelperType.DrbdProxy;
    }

    @Override
    public void manage(AutoHelperContext ctx)
    {
        try
        {
            Resource[] resources = getResourcesAsArray(ctx.rscDfn);

            for (int firstIdx = 0; firstIdx < resources.length; firstIdx++)
            {
                Resource firstRsc = resources[firstIdx];
                PriorityProps prioPropFirstRsc = getPrioProps(firstRsc);
                String siteA = prioPropFirstRsc.getProp(ApiConsts.KEY_SITE);
                if (siteA != null)
                {
                    for (int secondIdx = firstIdx + 1; secondIdx < resources.length; secondIdx++)
                    {
                        Resource secondRsc = resources[secondIdx];
                        PriorityProps prioPropSecondRsc = getPrioProps(secondRsc);
                        String siteB = prioPropSecondRsc.getProp(ApiConsts.KEY_SITE);

                        if (siteB != null && !siteA.equals(siteB))
                        {
                            if (isAutoDrbdProxyBeEnabled(firstRsc, secondRsc, ctx.responses))
                            {
                                enableDrbdProxyIfNotEnabledYet(firstRsc, secondRsc, ctx.responses);
                                // we do not disable proxy automatically. never.
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "for automtic enabling of DRBD proxy",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }

    }

    private void enableDrbdProxyIfNotEnabledYet(
        Resource firstRscRef,
        Resource secondRscRef,
        ApiCallRcImpl apiCallRcImplRef
    )
        throws AccessDeniedException
    {
        AccessContext peerCtx = peerCtxProvider.get();
        ResourceConnection rscConn = firstRscRef.getAbsResourceConnection(peerCtx, secondRscRef);

        if (
            rscConn == null ||
            !rscConn.getStateFlags().isSet(
                peerCtx,
                ResourceConnection.Flags.LOCAL_DRBD_PROXY
            )
        )
        {
            drbdProxyHelper.enableProxy(
                null, // no uuid to check against
                firstRscRef.getNode().getName().displayValue,
                secondRscRef.getNode().getName().displayValue,
                firstRscRef.getResourceDefinition().getName().displayValue,
                null // generate new tcp port for drbd proxy
            );
            if (rscConn == null)
            {
                rscConn = firstRscRef.getAbsResourceConnection(peerCtx, secondRscRef); // drbdProxyHelper should have
                                                                                       // created the rscConn
            }
            apiCallRcImplRef.addEntry(
                "Enabled drbd-proxy for " + rscConn.toString(),
                ApiConsts.INFO_AUTO_DRBD_PROXY_CREATED
            );
        }
    }

    private Resource[] getResourcesAsArray(ResourceDefinition rscDfnRef) throws AccessDeniedException
    {
        Resource[] rscs = new Resource[rscDfnRef.getResourceCount()];
        Iterator<Resource> iterateResource = rscDfnRef.iterateResource(peerCtxProvider.get());
        int idx = 0;
        while (iterateResource.hasNext())
        {
            Resource rsc = iterateResource.next();
            rscs[idx] = rsc;
            ++idx;
        }
        return rscs;
    }

    private boolean isAutoDrbdProxyBeEnabled(
        Resource firstRscRef,
        Resource secondRscRef,
        ApiCallRcImpl apiCallRcImplRef
    )
        throws AccessDeniedException
    {

        boolean hasFirstNodeAutoProxyEnabled = isProxySupported(firstRscRef.getNode());
        boolean hasSecondNodeAutoProxyEnabled = isProxySupported(secondRscRef.getNode());

        boolean isAutoProxyEnabled = false;
        if (hasFirstNodeAutoProxyEnabled && hasSecondNodeAutoProxyEnabled)
        {
            AccessContext peerCtx = peerCtxProvider.get();
            PriorityProps prioProps;

            ResourceConnection rscCon = firstRscRef.getAbsResourceConnection(peerCtx, secondRscRef);
            if (rscCon != null)
            {
                prioProps = getPrioProps(rscCon);
            }
            else
            {
                prioProps = getPrioProps(firstRscRef, secondRscRef);
            }
            String autoProxyVal = prioProps.getProp(
                ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE,
                ApiConsts.NAMESPC_DRBD_PROXY
            );
            isAutoProxyEnabled = autoProxyVal != null &&
                (autoProxyVal.equalsIgnoreCase(ApiConsts.VAL_TRUE) || autoProxyVal.equals(ApiConsts.VAL_YES));
        }
        return isAutoProxyEnabled;
    }

    /**
     * This method only returns true if the given node has support for drbd proyx
     *
     * @param nodeRef
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    private boolean isProxySupported(Node node) throws AccessDeniedException
    {
        AccessContext peerCtx = peerCtxProvider.get();
        ExtToolsInfo drbdProxyInfo = node.getPeer(peerCtx).getExtToolsManager().getExtToolInfo(
            ExtTools.DRBD_PROXY
        );
        return drbdProxyInfo != null && drbdProxyInfo.isSupported();
    }

    private PriorityProps getPrioProps(Resource rsc) throws AccessDeniedException
    {
        AccessContext peerCtx = peerCtxProvider.get();
        return new PriorityProps(
            rsc.getProps(peerCtx),
            rsc.getResourceDefinition().getProps(peerCtx),
            rsc.getResourceDefinition().getResourceGroup().getProps(peerCtx),
            rsc.getNode().getProps(peerCtx),
            systemConfRepository.getCtrlConfForView(peerCtx)
        );
    }

    private PriorityProps getPrioProps(Resource firstRsc, Resource secondRsc) throws AccessDeniedException
    {
        AccessContext peerCtx = peerCtxProvider.get();
        return new PriorityProps(
            firstRsc.getProps(peerCtx),
            secondRsc.getProps(peerCtx),
            firstRsc.getResourceDefinition().getProps(peerCtx),
            firstRsc.getNode().getProps(peerCtx),
            secondRsc.getNode().getProps(peerCtx),
            systemConfRepository.getCtrlConfForView(peerCtx)
        );
    }

    private PriorityProps getPrioProps(ResourceConnection rscCon) throws AccessDeniedException
    {
        AccessContext peerCtx = peerCtxProvider.get();
        Resource srcRsc = rscCon.getSourceResource(peerCtx);
        Resource targetRsc = rscCon.getTargetResource(peerCtx);
        return new PriorityProps(
            rscCon.getProps(peerCtx),
            srcRsc.getProps(peerCtx),
            targetRsc.getProps(peerCtx),
            srcRsc.getResourceDefinition().getProps(peerCtx),
            srcRsc.getNode().getProps(peerCtx),
            targetRsc.getNode().getProps(peerCtx),
            systemConfRepository.getCtrlConfForView(peerCtx)
        );
    }

}
