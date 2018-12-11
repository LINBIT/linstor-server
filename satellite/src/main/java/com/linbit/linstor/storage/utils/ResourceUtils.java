package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.Resource;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.List;

public class ResourceUtils
{
    public static Resource getSingleChild(Resource rsc, AccessContext accCtx) throws AccessDeniedException
    {
        List<Resource> childResources = rsc.getChildResources(accCtx);
        if (childResources.size() != 1)
        {
            throw new ImplementationError("Exactly one child-resource expected but found: " +
                childResources.size() + "\n" + childResources);
        }
        return childResources.get(0);
    }

}
