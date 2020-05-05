package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelper;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRscAutoHelper.AutoHelperInternalState;
import com.linbit.linstor.core.apicallhandler.response.ApiException;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Iterator;

@Singleton
public class CtrlRscAutoDrbdProxyHelper implements AutoHelper
{
    private final AccessContext apiCtx;
    private final CtrlRscConnectionHelper rscConnHelper;
    private final CtrlDrbdProxyHelper drbdProxyHelper;

    @Inject
    public CtrlRscAutoDrbdProxyHelper(
        @ApiContext AccessContext apiCtxRef,
        CtrlRscConnectionHelper rscConnHelperRef,
        CtrlDrbdProxyHelper drbdProxyHelperRef
    )
    {
        apiCtx = apiCtxRef;
        rscConnHelper = rscConnHelperRef;
        drbdProxyHelper = drbdProxyHelperRef;
    }

    @Override
    public void manage(
        ApiCallRcImpl apiCallRcImplRef,
        ResourceDefinition rscDfnRef,
        AutoHelperInternalState autoHelperInternalStateRef
    )
    {
        try
        {
            Resource[] resources = getResourcesAsArray(rscDfnRef);

            for (int firstIdx = 0; firstIdx < resources.length; firstIdx++)
            {
                Resource firstRsc = resources[firstIdx];
                String siteA = firstRsc.getNode().getProps(apiCtx).getProp(
                    ApiConsts.KEY_DRBD_PROXY_SITE,
                    ApiConsts.NAMESPC_DRBD_PROXY
                );
                if (siteA != null)
                {
                    for (int secondIdx = firstIdx + 1; secondIdx < resources.length; secondIdx++)
                    {
                        Resource secondRsc = resources[secondIdx];
                        String siteB = secondRsc.getNode().getProps(apiCtx).getProp(
                            ApiConsts.KEY_DRBD_PROXY_SITE,
                            ApiConsts.NAMESPC_DRBD_PROXY
                        );

                        if (siteB != null && !siteA.equals(siteB))
                        {
                            if (isAutoDrbdProxyBeEnabled(firstRsc, secondRsc, apiCallRcImplRef))
                            {
                                enableDrbdProxyIfNotEnabledYet(firstRsc, secondRsc, apiCallRcImplRef);
                                // we do not disable proxy automatically. never.
                            }
                        }
                    }
                }
            }
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }

    }

    private void enableDrbdProxyIfNotEnabledYet(
        Resource firstRscRef,
        Resource secondRscRef,
        ApiCallRcImpl apiCallRcImplRef
    )
        throws AccessDeniedException
    {
        ResourceConnection rscConn = firstRscRef.getAbsResourceConnection(apiCtx, secondRscRef);

        if (
            rscConn == null ||
            !rscConn.getStateFlags().isSet(
                apiCtx,
                ResourceConnection.Flags.LOCAL_DRBD_PROXY
            )
        )
        {
            drbdProxyHelper.enableProxy(
                null, // no uuid to check against
                firstRscRef.getNode().getName().displayValue,
                secondRscRef.getNode().getName().displayValue,
                firstRscRef.getDefinition().getName().displayValue,
                null // generate new tcp port for drbd proxy
            );
            apiCallRcImplRef.addEntry(
                "Enabled drbd-proxy for " + rscConn.toString(),
                ApiConsts.INFO_AUTO_DRBD_PROXY_CREATED
            );
        }
    }

    private Resource[] getResourcesAsArray(ResourceDefinition rscDfnRef) throws AccessDeniedException
    {
        Resource[] rscs = new Resource[rscDfnRef.getResourceCount()];
        Iterator<Resource> iterateResource = rscDfnRef.iterateResource(apiCtx);
        int i = 0;
        while (iterateResource.hasNext())
        {
            Resource rsc = iterateResource.next();
            rscs[i++] = rsc;
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

        boolean hasFirstNodeAutoProxyEnabled = hasNodeAutoProxyEnabled(
            firstRscRef.getNode(),
            apiCallRcImplRef
        );
        boolean hasSecondNodeAutoProxyEnabled = hasNodeAutoProxyEnabled(
            secondRscRef.getNode(),
            apiCallRcImplRef
        );

        boolean isAutoProxyEnabled = false;
        if (hasFirstNodeAutoProxyEnabled && hasSecondNodeAutoProxyEnabled)
        {
            ResourceConnection rscCon = firstRscRef.getAbsResourceConnection(apiCtx, secondRscRef);
            if (rscCon == null)
            {
                apiCallRcImplRef.addEntry(
                    "Enabled auto-drbd-proxy for resource connection between " + firstRscRef + " and " + secondRscRef + " by setting key " +
                        ApiConsts.NAMESPC_DRBD_PROXY + "/" + ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE + " to " +
                        ApiConsts.KEY_ENABLED,
                    ApiConsts.INFO_PROP_SET
                );
                rscCon = rscConnHelper.createRscConn(firstRscRef, secondRscRef, new ResourceConnection.Flags[0]);
                try
                {
                    rscCon.getProps(apiCtx).setProp(
                        ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE,
                        ApiConsts.VAL_TRUE,
                        ApiConsts.NAMESPC_DRBD_PROXY
                    );

                    isAutoProxyEnabled = true;
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
                catch (DatabaseException exc)
                {
                    throw new ApiException(exc);
                }
            }
            else
            {
                String autoProxyVal = rscCon.getProps(apiCtx).getProp(
                    ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE,
                    ApiConsts.NAMESPC_DRBD_PROXY
                );
                isAutoProxyEnabled = ApiConsts.VAL_TRUE.equalsIgnoreCase(autoProxyVal);
            }
        }
        return isAutoProxyEnabled;
    }

    /**
     * This method only returns true if the given node has support for drbd proyx AND
     * the node has either NOT set the property DrbdProxy/AutoEnable or the vlaue of that property
     * is enabled.
     *
     * @param nodeRef
     * @param apiCallRcImplRef
     *
     * @return
     *
     * @throws AccessDeniedException
     */
    private boolean hasNodeAutoProxyEnabled(Node nodeRef, ApiCallRcImpl apiCallRcImplRef) throws AccessDeniedException
    {
        boolean ret = false;

        ExtToolsInfo drbdProxyInfo = nodeRef.getPeer(apiCtx).getExtToolsManager().getExtToolInfo(ExtTools.DRBD_PROXY);
        boolean isDrbdProxySupported = drbdProxyInfo != null && drbdProxyInfo.isSupported();

        if (isDrbdProxySupported)
        {
            Props nodeProps = nodeRef.getProps(apiCtx);
            String autoEnabledValue = nodeProps.getProp(
                ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE,
                ApiConsts.NAMESPC_DRBD_PROXY
            );
            if (autoEnabledValue == null)
            {
                try
                {
                    nodeProps.setProp(
                        ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE,
                        ApiConsts.VAL_TRUE,
                        ApiConsts.NAMESPC_DRBD_PROXY
                    );
                    apiCallRcImplRef.addEntry(
                        "Enabled auto-drbd-proxy for " + nodeRef.toString() + " by setting key " +
                            ApiConsts.NAMESPC_DRBD_PROXY + "/" + ApiConsts.KEY_DRBD_PROXY_AUTO_ENABLE + " to " +
                            ApiConsts.VAL_TRUE,
                        ApiConsts.INFO_PROP_SET
                    );
                    ret = true;
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
                catch (DatabaseException exc)
                {
                    throw new ApiException(exc);
                }
            }
            else
            {
                ret = autoEnabledValue.equalsIgnoreCase(ApiConsts.VAL_TRUE) &&
                    nodeProps.getProp(ApiConsts.KEY_DRBD_PROXY_SITE, ApiConsts.NAMESPC_DRBD_PROXY) != null;
            }
        }
        return ret;
    }

}
