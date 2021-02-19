package com.linbit.linstor.core.devmgr.pojos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalNodePropsChangePojo
{
    public final Map<String, String> changedProps;
    public final Set<String> deletedProps;

    public LocalNodePropsChangePojo()
    {
        changedProps = new HashMap<>();
        deletedProps = new HashSet<>();
    }

    public LocalNodePropsChangePojo(Map<String, String> changedPropsRef, Set<String> deletedPropsRef)
    {
        changedProps = changedPropsRef;
        deletedProps = deletedPropsRef;
    }

    public void putAll(LocalNodePropsChangePojo pojo)
    {
        if (pojo != null)
        {
            changedProps.putAll(pojo.changedProps);
            deletedProps.addAll(pojo.deletedProps);
        }
    }
}
