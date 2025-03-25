package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.apicallhandler.response.ApiAccessDeniedException;
import com.linbit.linstor.core.apicallhandler.response.ApiDatabaseException;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.NetInterfaceName;
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
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.repository.SystemConfRepository;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
    private final SystemConfRepository systemConfRepository;

    @Inject
    public CtrlPropsHelper(
        WhitelistProps propsWhiteListRef,
        @PeerContext Provider<AccessContext> peerAccCtxRef,
        SystemConfRepository systemConfRepositoryRef
    )
    {
        propsWhiteList = propsWhiteListRef;
        peerAccCtx = peerAccCtxRef;
        systemConfRepository = systemConfRepositoryRef;
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

    public ReadOnlyProps getCtrlPropsForView()
    {
        return getCtrlPropsForView(peerAccCtx.get());
    }

    public ReadOnlyProps getCtrlPropsForView(AccessContext accessContextRef)
    {
        try
        {
            return systemConfRepository.getCtrlConfForView(accessContextRef);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access controller properties",
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG
            );
        }
    }

    public Props getCtrlPropsForChange()
    {
        return getCtrlPropsForChange(peerAccCtx.get());
    }

    public Props getCtrlPropsForChange(AccessContext accessContextRef)
    {
        try
        {
            return systemConfRepository.getCtrlConfForChange(accessContextRef);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access controller properties",
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG
            );
        }
    }

    public ReadOnlyProps getStltPropsForView()
    {
        return getStltPropsForView(peerAccCtx.get());
    }

    public ReadOnlyProps getStltPropsForView(AccessContext accessContextRef)
    {
        try
        {
            return systemConfRepository.getStltConfForView(accessContextRef);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ApiAccessDeniedException(
                accDeniedExc,
                "access satellite properties",
                ApiConsts.FAIL_ACC_DENIED_CTRL_CFG
            );
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
                "access properties for resource '" + rsc.getResourceDefinition().getName().displayValue + "' on node '" +
                    rsc.getNode().getName().displayValue + "'",
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

    public Props getProps(SnapshotDefinition snapDfn, boolean rscDfnProps)
    {
        return getProps(peerAccCtx.get(), snapDfn, rscDfnProps);
    }

    public Props getProps(AccessContext accCtx, SnapshotDefinition snapDfn, boolean rscDfnProps)
    {
        Props props;
        try
        {
            if (rscDfnProps)
            {
                props = snapDfn.getRscDfnPropsForChange(accCtx);
            }
            else
            {
                props = snapDfn.getSnapDfnProps(accCtx);
            }
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

    public Props getProps(SnapshotVolumeDefinition snapVlmDfn, boolean vlmDfnProps)
    {
        return getProps(peerAccCtx.get(), snapVlmDfn, vlmDfnProps);
    }

    public Props getProps(AccessContext accCtx, SnapshotVolumeDefinition snapVlmDfn, boolean vlmDfnProps)
    {
        Props props;
        try
        {
            if (vlmDfnProps)
            {
                props = snapVlmDfn.getVlmDfnPropsForChange(accCtx);
            }
            else
            {
                props = snapVlmDfn.getSnapVlmDfnProps(accCtx);
            }
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

    public Props getProps(Snapshot snap, boolean rscProps)
    {
        return getProps(peerAccCtx.get(), snap, rscProps);
    }

    public Props getProps(AccessContext accCtx, Snapshot snap, boolean rscProps)
    {
        Props props;
        try
        {
            if (rscProps)
            {
                props = snap.getRscPropsForChange(accCtx);
            }
            else
            {
                props = snap.getSnapProps(accCtx);
            }
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

    public Props getProps(SnapshotVolume snapVlm, boolean vlmProps)
    {
        return getProps(peerAccCtx.get(), snapVlm, vlmProps);
    }

    public Props getProps(AccessContext accCtx, SnapshotVolume snapVlm, boolean vlmProps)
    {
        Props props;
        try
        {
            if (vlmProps)
            {
                props = snapVlm.getVlmPropsForChange(accCtx);
            }
            else
            {
                props = snapVlm.getSnapVlmProps(accCtx);
            }
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

    public boolean fillProperties(
        ApiCallRcImpl apiCallRc,
        LinStorObject linstorObj,
        Map<String, String> sourceProps,
        Props targetProps,
        long failAccDeniedRc
    )
    {
        return fillProperties(
            apiCallRc,
            linstorObj,
            sourceProps,
            targetProps,
            failAccDeniedRc,
            new ArrayList<>(),
            new HashMap<>()
        );
    }

    public boolean fillProperties(
        ApiCallRcImpl apiCallRc,
        LinStorObject linstorObj,
        Map<String, String> sourceProps,
        Props targetProps,
        long failAccDeniedRc,
        List<String> ignoredKeysRef
    )
    {
        return fillProperties(
            apiCallRc,
            linstorObj,
            sourceProps,
            targetProps,
            failAccDeniedRc,
            ignoredKeysRef,
            new HashMap<>()
        );
    }

    /**
     *
     * @param apiCallRc For success/error messages
     * @param linstorObj What type of linstor obj the props should be checked(whitelist)
     * @param sourceProps Props to set
     * @param targetProps Current property container
     * @param failAccDeniedRc mask code of denied rc
     * @param propsChangedListenersRef
     * @param ignoredKeys keys to ignore for whitelistcheck
     *
     * @return true if properties were changed, otherwise false (e.g. setting the same value)
     */
    public boolean fillProperties(
        ApiCallRcImpl apiCallRc,
        LinStorObject linstorObj,
        Map<String, String> sourceProps,
        Props targetProps,
        long failAccDeniedRc,
        List<String> ignoredKeysRef,
        Map<String, PropertyChangedListener> propsChangedListenersRef
    )
    {
        boolean propsModified = false;
        ArrayList<String> ignoredKeys = new ArrayList<>(ignoredKeysRef);
        ignoredKeys.add(ApiConsts.NAMESPC_AUXILIARY + "/");

        for (Map.Entry<String, String> entry : sourceProps.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            boolean isPropAllowed = propsWhiteList.isAllowed(linstorObj, ignoredKeys, key, value, true);
            if (isPropAllowed)
            {
                String normalized = propsWhiteList.normalize(linstorObj, key, value);
                try
                {
                    final String oldVal = targetProps.setProp(key, normalized);
                    if (!normalized.equals(oldVal))
                    {
                        propsModified = true;
                    }
                    PropertyChangedListener listener = propsChangedListenersRef.get(key);
                    if (listener != null)
                    {
                        listener.changed(key, normalized, oldVal);
                    }

                    if (key.equalsIgnoreCase(
                        ApiConsts.NAMESPC_DRBD_OPTIONS + "/" + ApiConsts.KEY_DRBD_AUTO_QUORUM))
                    {
                        apiCallRc.add(ApiCallRcImpl.simpleEntry(ApiConsts.WARN_DEPRECATED,
                            key + " is deprecated, please use " +
                                ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM));
                    }

                    if (key.equalsIgnoreCase(
                        ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM))
                    {
                        targetProps.setProp(ApiConsts.KEY_QUORUM_SET_BY, "user", ApiConsts.NAMESPC_INTERNAL_DRBD);
                    }
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
                    .setDetails(propsWhiteList.getErrMsg(linstorObj, key))
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

        if (!sourceProps.isEmpty())
        {
            apiCallRc.addEntry(
                "Successfully set property key(s): " + String.join(",", sourceProps.keySet()),
                linstorObj.apiMask | ApiConsts.MASK_CRT | ApiConsts.CREATED
            );
        }
        return propsModified;
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

    public boolean remove(
        ApiCallRcImpl apiCallRc,
        LinStorObject linstorObj,
        Props props,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces
    )
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        return remove(
            apiCallRc,
            linstorObj,
            props,
            deletePropKeys,
            deleteNamespaces,
            new ArrayList<>(),
            new HashMap<>()
        );
    }

    public boolean remove(
        ApiCallRcImpl apiCallRc,
        LinStorObject linstorObj,
        Props props,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces,
        List<String> ignoredKeysRef
    )
        throws InvalidKeyException, AccessDeniedException, DatabaseException
    {
        return remove(
            apiCallRc,
            linstorObj,
            props,
            deletePropKeys,
            deleteNamespaces,
            ignoredKeysRef,
            new HashMap<>()
        );
    }

    /**
     * Remove a key from the property container
     *
     * @param apiCallRc
     * @param linstorObj
     * @param props
     * @param deletePropKeys
     * @param deleteNamespaces
     * @param propsChangedListenersRef
     *
     * @return true if a key was removed, otherwise false (e.g. key didn't exists at all)
     *
     * @throws AccessDeniedException
     * @throws InvalidKeyException
     * @throws DatabaseException
     */
    public boolean remove(
        ApiCallRcImpl apiCallRc,
        LinStorObject linstorObj,
        Props props,
        Collection<String> deletePropKeys,
        Collection<String> deleteNamespaces,
        List<String> ignoredKeysRef,
        Map<String, PropertyChangedListener> propsChangedListenersRef
    )
        throws AccessDeniedException, InvalidKeyException, DatabaseException
    {
        boolean propsModified = false;
        ArrayList<String> ignoredKeys = new ArrayList<>(ignoredKeysRef);
        ignoredKeys.add(ApiConsts.NAMESPC_AUXILIARY + "/");

        for (String key : deletePropKeys)
        {
            boolean isPropWhitelisted = propsWhiteList.isAllowed(linstorObj, ignoredKeys, key, null, false);
            if (isPropWhitelisted)
            {
                String deletedValue = props.removeProp(key);
                if (deletedValue != null)
                {
                    propsModified = true;
                }

                PropertyChangedListener listener = propsChangedListenersRef.get(key);
                if (listener != null)
                {
                    listener.changed(key, null, deletedValue);
                }

                if (key.equalsIgnoreCase(
                    ApiConsts.NAMESPC_DRBD_RESOURCE_OPTIONS + "/" + InternalApiConsts.KEY_DRBD_QUORUM))
                {
                    props.removeProp(ApiConsts.KEY_QUORUM_SET_BY, ApiConsts.NAMESPC_INTERNAL_DRBD);
                }
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
            @Nullable Props namespace = props.getNamespace(deleteNamespace);
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
                if (props.removeNamespace(deleteNamespace))
                {
                    propsModified = true;
                }
            }
            // else, noop
        }

        if (!deletePropKeys.isEmpty())
        {
            apiCallRc.addEntry(
                "Successfully deleted property key(s): " + String.join(",", deletePropKeys),
                linstorObj.apiMask | ApiConsts.MASK_DEL | ApiConsts.DELETED
            );
        }
        return propsModified;
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

    @FunctionalInterface
    public interface PropertyChangedListener
    {
        /**
         * The newValue after normalization, or null if property was deleted
         */
        void changed(String key, String newValue, String oldValue) throws AccessDeniedException, DatabaseException;
    }
}
