package com.linbit.linstor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.linbit.linstor.core.objects.NodeConnection;
import com.linbit.linstor.core.objects.ResourceConnection;
import com.linbit.linstor.core.objects.VolumeConnection;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public class PriorityProps
{
    private final List<Props> propList = new ArrayList<>();

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

    public void addFirstProps(Props prop)
    {
        if (prop != null)
        {
            propList.add(0, prop);
        }
    }

    public void addProps(Props prop)
    {
        if (prop != null)
        {
            propList.add(prop);
        }
    }

    public String getProp(String key, String namespace) throws InvalidKeyException
    {
        String value = null;
        for (Props prop : propList)
        {
            value = prop.getProp(key, namespace);
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
        for (Props prop : propList)
        {
            Optional<Props> optNs = prop.getNamespace(namespace);
            if (optNs.isPresent())
            {
                int nsLen = namespace.length();
                if (nsLen > 0)
                {
                    nsLen++; // also cut the trailing "/"
                }
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
}
