package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.NetInterfaceName;
import com.linbit.linstor.KeyValueStore;
import com.linbit.linstor.Node;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.StorPoolData;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    public void checkPrefNic(AccessContext accessContext, Node node, String prefNic, long maskObj)
            throws AccessDeniedException, InvalidNameException
    {
        if (prefNic != null)
        {
            if (node.getNetInterface(accessContext, new NetInterfaceName(prefNic)) == null)
            {
                throw new ApiRcException(ApiCallRcImpl.simpleEntry(
                        ApiConsts.MASK_ERROR | maskObj | ApiConsts.FAIL_INVLD_PROP,
                        "The network interface '" + prefNic + "' of node '" + node.getName() + "' does not exist!"
                ));
            }
        }
    }

    public Props getProps(Node node)
    {
        return getProps(peerAccCtx.get(), node);
    }

    public Props getProps(AccessContext accCtx, Node node)
    {
        Props props;
        try
        {
            props = node.getProps(accCtx);
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

    public Props getProps(StorPoolData storPool)
    {
        return getProps(peerAccCtx.get(), storPool);
    }

    public Props getProps(AccessContext accCtx, StorPoolData storPool)
    {
        Props props;
        try
        {
            props = storPool.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties of storage pool '" + storPool.getName().displayValue +
                    "' on node '" + storPool.getNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_STOR_POOL
            );
        }
        return props;
    }

    public Props getProps(ResourceDefinition rscDfn)
    {
        return getProps(peerAccCtx.get(), rscDfn);
    }

    public Props getProps(AccessContext accCtx, ResourceDefinition rscDfn)
    {
        Props props;
        try
        {
            props = rscDfn.getProps(accCtx);
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
        return getProps(peerAccCtx.get(), vlmDfn);
    }
    public Props getProps(AccessContext accCtx, VolumeDefinition vlmDfn)
    {
        Props props;
        try
        {
            props = vlmDfn.getProps(accCtx);
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
        return getProps(peerAccCtx.get(), rsc);
    }

    public Props getProps(AccessContext accCtx, Resource rsc)
    {
        Props props;
        try
        {
            props = rsc.getProps(accCtx);
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
        return getProps(peerAccCtx.get(), vlm);
    }

    public Props getProps(AccessContext accCtx, Volume vlm)
    {
        Props props;
        try
        {
            props = vlm.getProps(accCtx);
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

    public Props getProps(KeyValueStore kvs)
    {
        return getProps(peerAccCtx.get(), kvs);
    }

    public Props getProps(AccessContext accCtx, KeyValueStore kvs)
    {
        Props props;
        try
        {
            props = kvs.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                    accDeniedExc,
                    "access properties for keyValueStore '" + kvs.getName().displayValue + "'",
                    ApiConsts.FAIL_ACC_DENIED_KVS
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
        fillProperties(linstorObj, sourceProps, targetProps, failAccDeniedRc, new ArrayList<>());
    }

    public void fillProperties(
        LinStorObject linstorObj,
        Map<String, String> sourceProps,
        Props targetProps,
        long failAccDeniedRc,
        List<String> ignoredKeys
    )
    {
        for (Map.Entry<String, String> entry : sourceProps.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            ignoredKeys.add(ApiConsts.NAMESPC_AUXILIARY + "/");
            boolean isPropAllowed = propsWhiteList.isAllowed(linstorObj, ignoredKeys, key, value, true);
            if (isPropAllowed)
            {
                String normalized = propsWhiteList.normalize(linstorObj, key, value);
                try
                {
                    targetProps.setProp(key, normalized);
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
                    if (key.startsWith(ApiConsts.NAMESPC_AUXILIARY + "/"))
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
                    if (key.startsWith(ApiConsts.NAMESPC_AUXILIARY + "/"))
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

    public void addModifyDeleteUnconditional(
        Props props,
        Map<String, String> overrideProps,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces
    )
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, SQLException
    {
        for (Map.Entry<String, String> entry : overrideProps.entrySet())
        {
            props.setProp(entry.getKey(), entry.getValue());
        }
        remove(props, deletePropKeys, deleteNamespaces);
    }

    public void remove(
        Props props,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces
    )
        throws AccessDeniedException, InvalidKeyException, SQLException
    {
        for (String key : deletePropKeys)
        {
            props.removeProp(key);
        }
        for (String deleteNamespace : deleteNamespaces)
        {
            props.removeNamespace(deleteNamespace);
        }
    }
}
