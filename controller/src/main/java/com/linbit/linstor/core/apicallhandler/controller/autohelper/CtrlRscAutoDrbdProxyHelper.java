package com.linbit.linstor.core.apicallhandler.controller.autohelper;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlDrbdProxyHelper;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Iterator;

@Singleton
public class CtrlRscAutoDrbdProxyHelper implements AutoHelper
{
    private final CtrlDrbdProxyHelper drbdProxyHelper;
    private final SystemConfRepository systemConfRepository;

    @Inject
    CtrlRscAutoDrbdProxyHelper(
        CtrlDrbdProxyHelper drbdProxyHelperRef,
        SystemConfRepository systemConfRepositoryRef
    )
    {
        drbdProxyHelper = drbdProxyHelperRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    @Override
    public AutoHelperType getType()
    {
        return AutoHelperType.DRBD_PROXY;
    }

    @Override
    public void manage(AutoHelperContext ctx)
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
                        if (isAutoDrbdProxyBeEnabled(firstRsc, secondRsc))
                        {
                            enableDrbdProxyIfNotEnabledYet(firstRsc, secondRsc, ctx.responses);
                            // we do not disable proxy automatically. never.
                        }
                    }
                }
            }
        }

    }

    private void enableDrbdProxyIfNotEnabledYet(
        Resource firstRscRef,
        Resource secondRscRef,
        ApiCallRcImpl apiCallRcImplRef
    )
    {
        ResourceConnection rscConn = firstRscRef.getAbsResourceConnection(secondRscRef);

        if (
            rscConn == null ||
            !rscConn.getStateFlags().isSet(
                ResourceConnection.Flags.LOCAL_DRBD_PROXY
                )
        )
        {
            drbdProxyHelper.enableProxy(
                null, // no uuid to check against
                firstRscRef.getNode().getName().displayValue,
                secondRscRef.getNode().getName().displayValue,
                firstRscRef.getResourceDefinition().getName().displayValue,
                null, // generate new source tcp port for drbd proxy
                null // generate new target tcp port for drbd proxy
            );
            if (rscConn == null)
            {
                // drbdProxyHelper should have created the rscConn
                rscConn = firstRscRef.getAbsResourceConnection(secondRscRef);
            }
            apiCallRcImplRef.addEntry(
                "Enabled drbd-proxy for " + rscConn.toString(),
                ApiConsts.INFO_AUTO_DRBD_PROXY_CREATED
            );
        }
    }

    private Resource[] getResourcesAsArray(ResourceDefinition rscDfnRef)
    {
        Resource[] rscs = new Resource[rscDfnRef.getResourceCount()];
        Iterator<Resource> iterateResource = rscDfnRef.iterateResource();
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
        Resource secondRscRef
    )
    {

        boolean hasFirstNodeAutoProxyEnabled = isProxySupported(firstRscRef.getNode());
        boolean hasSecondNodeAutoProxyEnabled = isProxySupported(secondRscRef.getNode());

        boolean isAutoProxyEnabled = false;
        if (hasFirstNodeAutoProxyEnabled && hasSecondNodeAutoProxyEnabled)
        {
            PriorityProps prioProps;

            ResourceConnection rscCon = firstRscRef.getAbsResourceConnection(secondRscRef);
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
     *
     *
     */
    private boolean isProxySupported(Node node)
    {
        ExtToolsInfo drbdProxyInfo = node.getPeer().getExtToolsManager().getExtToolInfo(
            ExtTools.DRBD_PROXY
        );
        return drbdProxyInfo != null && drbdProxyInfo.isSupported();
    }

    private PriorityProps getPrioProps(Resource rsc)
    {
        return new PriorityProps(
            rsc.getProps(),
            rsc.getResourceDefinition().getProps(),
            rsc.getResourceDefinition().getResourceGroup().getProps(),
            rsc.getNode().getProps(),
            systemConfRepository.getCtrlConfForView()
        );
    }

    private PriorityProps getPrioProps(Resource firstRsc, Resource secondRsc)
    {
        return new PriorityProps(
            firstRsc.getProps(),
            secondRsc.getProps(),
            firstRsc.getResourceDefinition().getProps(),
            firstRsc.getNode().getProps(),
            secondRsc.getNode().getProps(),
            systemConfRepository.getCtrlConfForView()
        );
    }

    private PriorityProps getPrioProps(ResourceConnection rscCon)
    {
        Resource srcRsc = rscCon.getSourceResource();
        Resource targetRsc = rscCon.getTargetResource();
        return new PriorityProps(
            rscCon.getProps(),
            srcRsc.getProps(),
            targetRsc.getProps(),
            srcRsc.getResourceDefinition().getProps(),
            srcRsc.getNode().getProps(),
            targetRsc.getNode().getProps(),
            systemConfRepository.getCtrlConfForView()
        );
    }

}
