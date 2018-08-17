package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.apicallhandler.response.ApiSQLException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.Map;

@Singleton
public class CtrlPropsHelper
{
    private final WhitelistProps propsWhiteList;
    private final Provider<AccessContext> peerAccCtx;

    @Inject
    public CtrlPropsHelper(
        WhitelistProps propsWhiteListRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef
        )
    {
        propsWhiteList = propsWhiteListRef;
        peerAccCtx = peerAccCtxRef;
    }

    public Props getProps(Node node)
    {
        Props props;
        try
        {
            props = node.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for node '" + node.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_NODE
            );
        }
        return props;
    }

    public Props getProps(ResourceDefinition rscDfn)
    {
        Props props;
        try
        {
            props = rscDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for resource definition '" + rscDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_DFN
            );
        }
        return props;
    }

    public Props getProps(VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for volume definition with number '" + vlmDfn.getVolumeNumber().value + "' " +
                    "on resource definition '" + vlmDfn.getResourceDefinition().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM_DFN
            );
        }
        return props;
    }

    public Props getProps(Resource rsc)
    {
        Props props;
        try
        {
            props = rsc.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for resource '" + rsc.getDefinition().getName().displayValue + "' on node '" +
                    rsc.getAssignedNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return props;
    }

    public Props getProps(Volume vlm)
    {
        Props props;
        try
        {
            props = vlm.getProps(peerAccCtx.get());
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for volume with number '" + vlm.getVolumeDefinition().getVolumeNumber().value +
                    "' on resource '" + vlm.getResourceDefinition().getName().displayValue + "' " +
                    "on node '" + vlm.getResource().getAssignedNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_VLM
            );
        }
        return props;
    }

    public void fillProperties(
        LinStorObject linstorObj,
        Map<String, String> sourceProps,
        Props targetProps,
        long failAccDeniedRc
    )
    {
        for (Map.Entry<String, String> entry : sourceProps.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();
            boolean isAuxProp = key.startsWith(ApiConsts.NAMESPC_AUXILIARY + "/");

            // boolean isPropAllowed = true;
            boolean isPropAllowed =
                isAuxProp ||
                    propsWhiteList.isAllowed(linstorObj, key, value, true);
            if (isPropAllowed)
            {
                try
                {
                    targetProps.setProp(key, value);
                }
                catch (AccessDeniedException exc)
                {
                    throw new ApiAccessDeniedException(
                        exc,
                        "insert property '" + key + "'",
                        failAccDeniedRc
                    );
                }
                catch (InvalidKeyException exc)
                {
                    if (isAuxProp)
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid key.")
                            .setCause("The key '" + key + "' is invalid.")
                            .build(),
                            exc
                        );
                    }
                    else
                    {
                        // we tried to insert an invalid but whitelisted key
                        throw new ImplementationError(exc);
                    }
                }
                catch (InvalidValueException exc)
                {
                    if (isAuxProp)
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid value.")
                            .setCause("The value '" + value + "' is invalid.")
                            .build(),
                            exc
                        );
                    }
                    else
                    {
                        // we tried to insert an invalid but whitelisted value
                        throw new ImplementationError(exc);
                    }
                }
                catch (SQLException exc)
                {
                    throw new ApiSQLException(exc);
                }
            }
            else
            if (propsWhiteList.isKeyKnown(linstorObj, key))
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property value")
                    .setCause("The value '" + value + "' is not valid for the key '" + key + "'")
                    .setDetails("The value must match '" + propsWhiteList.getRuleValue(linstorObj, key) + "'")
                    .build()
                );
            }
            else
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key")
                    .setCause("The key '" + key + "' is not whitelisted.")
                    .build()
                );
            }
        }
    }
}
