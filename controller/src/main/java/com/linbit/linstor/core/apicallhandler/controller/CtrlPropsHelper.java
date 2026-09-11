package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SizeSpecParser;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.prop.LinStorObject;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.apicallhandler.controller.autoplacer.Autoplacer;
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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

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
    /**
     * The client command first (review request on the MR: these errors are usually hit from
     * the command line), the API endpoint as fallback for GUI / API users.
     */
    public static final String CORRECTION_INVALID_PROP_KEY =
        "Refer to 'linstor <object> set-property --help' for a list of valid property keys, " +
        "or query the object's 'properties/info' API endpoint.";

    private final WhitelistProps propsWhiteList;
    private final SystemConfRepository systemConfRepository;

    public static String causeInvalidPropKey(String key)
    {
        return "The key '" + key + "' is not a valid property key.";
    }

    @Inject
    public CtrlPropsHelper(
        WhitelistProps propsWhiteListRef,
        SystemConfRepository systemConfRepositoryRef
    )
    {
        propsWhiteList = propsWhiteListRef;
        systemConfRepository = systemConfRepositoryRef;
    }

    public void checkPrefNic(Node node, String prefNic, long maskObj)
        throws InvalidNameException
    {
        checkSpecialNic(node, prefNic, maskObj, false);
    }

    public void checkPrefOutsideAddress(Node node, String outsideAddress, long maskObj)
        throws InvalidNameException
    {
        checkSpecialNic(node, outsideAddress, maskObj, true);
    }

    private void checkSpecialNic(
        Node node,
        String specialNic,
        long maskObj,
        boolean allowEmptyString
    )
        throws InvalidNameException
    {
        if (specialNic != null && !(specialNic.isEmpty() && allowEmptyString))
        {
            if (node.getNetInterface(new NetInterfaceName(specialNic)) == null)
            {
                throw new ApiRcException(
                    ApiCallRcImpl.simpleEntry(
                        ApiConsts.MASK_ERROR | maskObj | ApiConsts.FAIL_INVLD_PROP,
                        "The network interface '" + specialNic + "' of node '" + node.getName() +
                            "' does not exist!"
                    )
                );
            }
        }
    }


    public ReadOnlyProps getCtrlPropsForView()
    {
        return systemConfRepository.getCtrlConfForView();
    }


    public Props getCtrlPropsForChange()
    {
        return systemConfRepository.getCtrlConfForChange();
    }


    public ReadOnlyProps getStltPropsForView()
    {
        return systemConfRepository.getStltConfForView();
    }


    public Props getProps(Node node)
    {
        Props props;
        props = node.getProps();
        return props;
    }


    public Props getProps(StorPool storPool)
    {
        Props props;
        props = storPool.getProps();
        return props;
    }


    public Props getProps(ResourceDefinition rscDfn)
    {
        Props props;
        props = rscDfn.getProps();
        return props;
    }


    public Props getProps(ResourceGroup rscGrp)
    {
        Props props;
        props = rscGrp.getProps();
        return props;
    }


    public Props getProps(VolumeDefinition vlmDfn)
    {
        Props props;
        props = vlmDfn.getProps();
        return props;
    }


    public Props getProps(Resource rsc)
    {
        Props props;
        props = rsc.getProps();
        return props;
    }


    public Props getProps(Volume vlm)
    {
        Props props;
        props = vlm.getProps();
        return props;
    }


    public Props getProps(KeyValueStore kvs)
    {
        Props props;
        props = kvs.getProps();
        return props;
    }


    public Props getProps(SnapshotDefinition snapDfn, boolean rscDfnProps)
    {
        Props props;
        if (rscDfnProps)
        {
            props = snapDfn.getRscDfnPropsForChange();
        }
        else
        {
            props = snapDfn.getSnapDfnProps();
        }
        return props;
    }


    public Props getProps(SnapshotVolumeDefinition snapVlmDfn, boolean vlmDfnProps)
    {
        Props props;
        if (vlmDfnProps)
        {
            props = snapVlmDfn.getVlmDfnPropsForChange();
        }
        else
        {
            props = snapVlmDfn.getSnapVlmDfnProps();
        }
        return props;
    }


    public Props getProps(Snapshot snap, boolean rscProps)
    {
        Props props;
        if (rscProps)
        {
            props = snap.getRscPropsForChange();
        }
        else
        {
            props = snap.getSnapProps();
        }
        return props;
    }


    public Props getProps(SnapshotVolume snapVlm, boolean vlmProps)
    {
        Props props;
        if (vlmProps)
        {
            props = snapVlm.getVlmPropsForChange();
        }
        else
        {
            props = snapVlm.getSnapVlmProps();
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
            Collections.emptyList(),
            Collections.emptyMap()
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
     * Fills the target property container with values from source properties after whitelist validation.
     *
     * @param apiCallRc For success/error messages
     * @param linstorObj What type of linstor obj the props should be checked(whitelist)
     * @param sourceProps Props to set
     * @param targetProps Current property container
     * @param failAccDeniedRc mask code of denied rc
     * @param ignoredKeysRef keys to ignore for whitelistcheck
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

                    if (key.equals(Autoplacer.MIN_FREE_SPACE_PROP))
                    {
                        SizeSpecParser.ensureParsableWithPercent(normalized);
                    }
                }
                catch (InvalidKeyException exc)
                {
                    if (key.startsWith(ApiConsts.NAMESPC_AUXILIARY + "/"))
                    {
                        throw new ApiRcException(ApiCallRcImpl
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid key: " + key)
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
                            .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid value: " + value)
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
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP,
                        String.format("Invalid property(%s) value: %s", key, value))
                    .setCause("The value '" + value + "' is not valid for the key '" + key + "'")
                    .setDetails(propsWhiteList.getErrMsg(linstorObj, key))
                    .build()
                );
            }
            else
            {
                throw new ApiRcException(ApiCallRcImpl
                    .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key: " + key)
                    .setCause(causeInvalidPropKey(key))
                    .setCorrection(CORRECTION_INVALID_PROP_KEY)
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
        throws InvalidKeyException, InvalidValueException, DatabaseException
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
        throws InvalidKeyException, DatabaseException
    {
        return remove(
            apiCallRc,
            linstorObj,
            props,
            deletePropKeys,
            deleteNamespaces,
            Collections.emptyList(),
            Collections.emptyMap()
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
        throws InvalidKeyException, DatabaseException
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
     *
     * @return true if a key was removed, otherwise false (e.g. key didn't exists at all)
     *
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
        throws InvalidKeyException, DatabaseException
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
                        .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key: " + key)
                        .setCause(causeInvalidPropKey(key))
                        .setCorrection(CORRECTION_INVALID_PROP_KEY)
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
                                .entryBuilder(ApiConsts.FAIL_INVLD_PROP, "Invalid property key: " + key)
                                .setCause(causeInvalidPropKey(key))
                                .setCorrection(CORRECTION_INVALID_PROP_KEY)
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
        throws InvalidKeyException, DatabaseException
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
     * Copies properties from source to destination with optional retain and override behavior.
     *
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
        void changed(String key, @Nullable String newValue, @Nullable String oldValue) throws DatabaseException;
    }
}
