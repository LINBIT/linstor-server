package com.linbit.linstor.core.devmgr.pojos;

import java.util.Map;
import java.util.Set;

public class LocalNodePropsChangePojo
{
    public final Map<String, String> changedProps;
    public final Set<String> deletedProps;

    public LocalNodePropsChangePojo(Map<String, String> changedPropsRef, Set<String> deletedPropsRef)
    {
        changedProps = changedPropsRef;
        deletedProps = deletedPropsRef;
    }
}
