package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

    public Props getProps(StorPool storPool)
    {
        return getProps(peerAccCtx.get(), storPool);
    }

    public Props getProps(AccessContext accCtx, StorPool storPool)
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

    public Props getProps(ResourceGroup rscGrp)
    {
        return getProps(peerAccCtx.get(), rscGrp);
    }

    public Props getProps(AccessContext accCtx, ResourceGroup rscGrp)
    {
        Props props;
        try
        {
            props = rscGrp.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for resource group '" + rscGrp.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC_GRP
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
                    rsc.getNode().getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_RSC
            );
        }
        return props;
    }

    public Props getProps(AbsVolume<?> vlm)
    {
        return getProps(peerAccCtx.get(), vlm);
    }

    public Props getProps(AccessContext accCtx, AbsVolume<?> vlm)
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
                    "on node '" + vlm.getAbsResource().getNode().getName().displayValue + "'",
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

    public Props getProps(SnapshotDefinition snapDfn)
    {
        return getProps(peerAccCtx.get(), snapDfn);
    }

    public Props getProps(AccessContext accCtx, SnapshotDefinition snapDfn)
    {
        Props props;
        try
        {
            props = snapDfn.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for snapshot definition, resource name: '" +
                    snapDfn.getResourceName().displayValue + "', snapshot name: '" +
                    snapDfn.getName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return props;
    }

    public Props getProps(SnapshotVolumeDefinition snapVlmDfn)
    {
        return getProps(peerAccCtx.get(), snapVlmDfn);
    }

    public Props getProps(AccessContext accCtx, SnapshotVolumeDefinition snapVlmDfn)
    {
        Props props;
        try
        {
            props = snapVlmDfn.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for snapshot definition, resource name: '" +
                    snapVlmDfn.getResourceName().displayValue + "', snapshot name: '" +
                    snapVlmDfn.getSnapshotName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return props;
    }

    public Props getProps(Snapshot snap)
    {
        return getProps(peerAccCtx.get(), snap);
    }

    public Props getProps(AccessContext accCtx, Snapshot snap)
    {
        Props props;
        try
        {
            props = snap.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for snapshot definition, resource name: '" +
                    snap.getResourceName().displayValue + "', snapshot name: '" +
                    snap.getSnapshotName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
            );
        }
        return props;
    }

    public Props getProps(SnapshotVolume snapVlm)
    {
        return getProps(peerAccCtx.get(), snapVlm);
    }

    public Props getProps(AccessContext accCtx, SnapshotVolume snapVlm)
    {
        Props props;
        try
        {
            props = snapVlm.getProps(accCtx);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access properties for snapshot definition, resource name: '" +
                    snapVlm.getResourceName().displayValue + "', snapshot name: '" +
                    snapVlm.getSnapshotName().displayValue + "'",
                ApiConsts.FAIL_ACC_DENIED_SNAP_DFN
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
                catch (DatabaseException exc)
                {
                    throw new ApiDatabaseException(exc);
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
        throws AccessDeniedException, InvalidKeyException, InvalidValueException, DatabaseException
    {
        for (Map.Entry<String, String> entry : overrideProps.entrySet())
        {
            props.setProp(entry.getKey(), entry.getValue());
        }
        removeUnconditional(props, deletePropKeys, deleteNamespaces);
    }

    public void remove(
        LinStorObject linstorObj,
        Props props,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces
    )
        throws AccessDeniedException, InvalidKeyException, DatabaseException
    {
        List<String> ignoredKeys = Arrays.asList(ApiConsts.NAMESPC_AUXILIARY + "/");

        for (String key : deletePropKeys)
        {
            boolean isPropWhitelisted = propsWhiteList.isAllowed(linstorObj, ignoredKeys, key, null, false);
            if (isPropWhitelisted)
            {
                props.removeProp(key);
            }
            else
            {
                throw new ApiRcException(
                    ApiCallRcImpl
                        .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key")
                        .setCause("The key '" + key + "' is not whitelisted.")
                        .build()
                );
            }
        }
        for (String deleteNamespace : deleteNamespaces)
        {
            Props namespace = props.getNamespace(deleteNamespace).orElse(null);
            if (namespace != null)
            {
                Set<String> keySet = namespace.keySet();
                for (String key : keySet)
                {
                    boolean isPropWhitelisted = propsWhiteList.isAllowed(linstorObj, ignoredKeys, key, null, false);
                    if (!isPropWhitelisted)
                    {
                        throw new ApiRcException(
                            ApiCallRcImpl
                                .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key")
                                .setCause("The key '" + key + "' is not whitelisted.")
                                .build()
                        );
                    }
                }
                props.removeNamespace(deleteNamespace);
            }
            // else, noop
        }
    }

    public void removeUnconditional(
        Props props,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces
    )
        throws InvalidKeyException, AccessDeniedException, DatabaseException
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

    public void copy(Props fromProps, Props toProps)
    {
        copy(fromProps, toProps, false, true);
    }

    /**
     * @param sourceProp
     *     source Props
     * @param destinationProps
     *     destination Props
     * @param retain
     *     if true, all properties in destination will be removed that do not exist in
     *     the source props
     * @param override
     *     if true, source properties will override existing destination properties
     */
    public void copy(Props sourceProp, Props destinationProps, boolean retain, boolean override)
    {
        Map<String, String> srcMap = sourceProp.map();
        Map<String, String> dstMap = destinationProps.map();

        Set<String> keysToDelete;
        if (retain)
        {
            keysToDelete = new HashSet<>(dstMap.keySet());
        }
        else
        {
            keysToDelete = Collections.emptySet();
        }
        for (Entry<String, String> srcEntry : srcMap.entrySet())
        {
            String key = srcEntry.getKey();
            if (override || !dstMap.containsKey(key))
            {
                dstMap.put(key, srcEntry.getValue());
            }
            keysToDelete.remove(key);
        }
        dstMap.keySet().removeAll(keysToDelete);
    }
}
