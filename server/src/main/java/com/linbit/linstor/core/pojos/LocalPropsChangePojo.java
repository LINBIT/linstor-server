package com.linbit.linstor.core.pojos;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.interfaces.StorPoolInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalPropsChangePojo
{
    public final Map<String, String> changedNodeProps;
    public final Set<String> deletedNodeProps;

    public final Map<StorPoolName, Map<String, String>> changedStorPoolProps;
    public final Map<StorPoolName, Set<String>> deletedStorPoolProps;

    /**
     * Combines / merges all give non-null {@link LocalPropsChangePojo}s. If all given {@link LocalPropsChangePojo}s are
     * null, <code>null</code> is returned.
     */
    public static @Nullable LocalPropsChangePojo merge(LocalPropsChangePojo... localPropsChangesToMerge)
    {
        boolean hasContent = false;
        for (@Nullable LocalPropsChangePojo item : localPropsChangesToMerge)
        {
            if (item != null)
            {
                hasContent = true;
                break;
            }
        }

        @Nullable LocalPropsChangePojo ret;
        if (hasContent)
        {
            ret = new LocalPropsChangePojo();
            for (LocalPropsChangePojo item : localPropsChangesToMerge)
            {
                ret.putAll(item);
            }
        }
        else
        {
            ret = null;
        }
        return ret;
    }

    public LocalPropsChangePojo()
    {
        changedNodeProps = new HashMap<>();
        changedStorPoolProps = new HashMap<>();
        deletedNodeProps = new HashSet<>();
        deletedStorPoolProps = new HashMap<>();
    }

    /**
     * Copies the entries from the parameter into this.
     *
     * @param pojo
     */
    public void putAll(@Nullable LocalPropsChangePojo pojo)
    {
        if (pojo != null)
        {
            copy(
                pojo.changedNodeProps,
                pojo.deletedNodeProps,
                changedNodeProps,
                deletedNodeProps
            );

            HashSet<StorPoolName> spNames = new HashSet<>();
            spNames.addAll(pojo.changedStorPoolProps.keySet());
            spNames.addAll(pojo.deletedStorPoolProps.keySet());
            for (StorPoolName spName : spNames)
            {
                copy(
                    pojo.changedStorPoolProps.get(spName),
                    pojo.deletedStorPoolProps.get(spName),
                    lazyGetMap(changedStorPoolProps, spName),
                    lazyGetSet(deletedStorPoolProps, spName)
                );
            }
        }
    }

    private Map<String, String> lazyGetMap(
        Map<StorPoolName, Map<String, String>> mapRef,
        StorPoolName keyRef
    )
    {
        Map<String, String> ret = mapRef.get(keyRef);
        if (ret == null)
        {
            ret = new HashMap<>();
            mapRef.put(keyRef, ret);
        }
        return ret;
    }

    private Set<String> lazyGetSet(
        Map<StorPoolName, Set<String>> mapRef,
        StorPoolName keyRef
    )
    {
        Set<String> ret = mapRef.get(keyRef);
        if (ret == null)
        {
            ret = new HashSet<>();
            mapRef.put(keyRef, ret);
        }
        return ret;
    }

    private void copy(
        @Nullable Map<String, String> changedPropsSrcRef,
        @Nullable Set<String> deletedPropsSrcRef,
        Map<String, String> changedPropsDstRef,
        Set<String> deletedPropsDstRef
    )
    {
        if (changedPropsSrcRef != null)
        {
            changedPropsDstRef.putAll(changedPropsSrcRef);
        }
        if (deletedPropsSrcRef != null)
        {
            deletedPropsDstRef.addAll(deletedPropsSrcRef);
        }
    }

    public boolean isEmpty()
    {
        return changedNodeProps.isEmpty() && deletedNodeProps.isEmpty() &&
            changedStorPoolProps.isEmpty() && deletedStorPoolProps.isEmpty();
    }

    public void changeStorPoolProp(StorPoolInfo storPoolRef, String key, String value)
    {
        lazyGetMap(changedStorPoolProps, storPoolRef.getName()).put(key, value);
    }

    public void deleteStorPoolProp(StorPoolInfo storPoolRef, String key)
    {
        lazyGetSet(deletedStorPoolProps, storPoolRef.getName()).add(key);
    }
}
