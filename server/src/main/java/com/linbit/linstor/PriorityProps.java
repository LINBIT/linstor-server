package com.linbit.linstor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;

import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.Pair;

public class PriorityProps
{
    private final List<Pair<Props, String>> propList = new ArrayList<>();

    public PriorityProps(
        AccessContext accCtx,
        NodeConnection nodeConnection,
        ResourceConnection resourceConnection,
        VolumeConnection volumeConnection
    )
        throws AccessDeniedException
    {
        if (volumeConnection != null)
        {
            addProps(volumeConnection.getProps(accCtx));
        }
        if (resourceConnection != null)
        {
            addProps(resourceConnection.getProps(accCtx));
        }
        if (nodeConnection != null)
        {
            addProps(nodeConnection.getProps(accCtx));
        }
    }

    /**
     * First Props is queried first in case of getProp(String, String)
     *
     * @param props
     */
    public PriorityProps(Props... props)
    {
        for (Props prop : props)
        {
            addProps(prop);
        }
    }

    public PriorityProps addProps(Props prop)
    {
        if (prop != null)
        {
            propList.add(new Pair<>(prop, ""));
        }
        return this;
    }

    public PriorityProps addProps(Props props, String descr)
    {
        if (props != null)
        {
            propList.add(new Pair<>(props, descr));
        }
        return this;
    }

    public String getProp(String key, String namespace) throws InvalidKeyException
    {
        String value = null;
        for (Pair<Props, String> pair : propList)
        {
            value = pair.objA.getProp(key, namespace);
            if (value != null)
            {
                break;
            }
        }
        return value;
    }

    public String getProp(String key, String namespace, String defaultValue) throws InvalidKeyException
    {
        final String val = getProp(key, namespace);
        return val == null ? defaultValue : val;
    }

    public String getProp(String key) throws InvalidKeyException
    {
        return getProp(key, null);
    }

    public Map<String, String> renderRelativeMap(String namespace)
    {
        Map<String, String> ret = new HashMap<>();

        int nsLen = namespace.length();
        if (nsLen > 0)
        {
            nsLen++; // also cut the trailing "/"
        }

        for (Pair<Props, String> prop : propList)
        {
            Optional<Props> optNs = prop.objA.getNamespace(namespace);
            if (optNs.isPresent())
            {
                for (Entry<String, String> entry : optNs.get().map().entrySet())
                {
                    ret.putIfAbsent(
                        entry.getKey().substring(nsLen),
                        entry.getValue()
                    );
                }
            }
        }
        return ret;
    }

    /**
     * @param namespcDrbdHandlerOptionsRef
     * @return true if any of the given {@link PropsContainer} has the given namespace, i.e. at least
     * one key/value within the given namespace
     */
    public boolean anyPropsHasNamespace(String namespcDrbdHandlerOptionsRef)
    {
        boolean ret = false;
        for (Pair<Props, String> entry : propList)
        {
            if (entry.objA.getNamespace(namespcDrbdHandlerOptionsRef).isPresent())
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    /**
     * Returns a list of <Value, PropsContainer-Description> of all property containers containing
     * the key. If a property container does not contain the requested key, it will be skipped from the
     * result.
     */
    public MultiResult getConflictingProp(String key, String namespace)
        throws InvalidKeyException
    {
        MultiResult ret = null;

        for (Pair<Props, String> pair : propList)
        {
            String value = pair.objA.getProp(key, namespace);
            if (value != null)
            {
                if (ret == null)
                {
                    ret = new MultiResult(value, pair.objB);
                }
                else
                {
                    ret.addResult(value, pair.objB);
                }
            }
        }

        return ret;
    }

    public Map<String, MultiResult> renderConflictingMap(String namespace, boolean absoluteKey)
    {
        Map<String, MultiResult> ret = new TreeMap<>();

        int nsLen = namespace.length();
        if (nsLen > 0)
        {
            nsLen++; // also cut the trailing "/"
        }

        for (Pair<Props, String> propWithDescr : propList)
        {
            Optional<Props> optNs = propWithDescr.objA.getNamespace(namespace);
            if (optNs.isPresent())
            {
                for (Entry<String, String> entry : optNs.get().map().entrySet())
                {
                    String key;
                    String absKey = entry.getKey();
                    if (absoluteKey)
                    {
                        key = absKey;
                    }
                    else
                    {
                        key = absKey.substring(nsLen);
                    }
                    MultiResult result = ret.get(key);
                    if (result == null)
                    {
                        result = new MultiResult(entry.getValue(), propWithDescr.objB);
                        ret.put(key, result);
                    }
                    else
                    {
                        result.addResult(entry.getValue(), propWithDescr.objB);
                    }
                }
            }
        }

        return ret;
    }

    public static class MultiResult
    {
        public final ValueWithDescirption first;
        public final List<ValueWithDescirption> conflictingList;
        private final List<ValueWithDescirption> modifiableList = new ArrayList<>();

        public MultiResult(String valueRef, String propDescr)
        {
            first = new ValueWithDescirption(valueRef, propDescr);
            conflictingList = Collections.unmodifiableList(modifiableList);
        }

        private void addResult(String valueRef, String propDescr)
        {
            modifiableList.add(new ValueWithDescirption(valueRef, propDescr));
        }
    }

    public static class ValueWithDescirption
    {
        public final String value;
        public final String propsDescription;

        private ValueWithDescirption(String valueRef, String propsDescriptionRef)
        {
            value = valueRef;
            propsDescription = propsDescriptionRef;
        }
    }
}
