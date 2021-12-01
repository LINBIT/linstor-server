package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ExceptionThrowingPredicate;

import java.util.Iterator;

public class ResourceDefinitionUtils
{
    public static int getResourceCount(
        AccessContext accCtx,
        ResourceDefinition rscDfn,
        ExceptionThrowingPredicate<Resource, AccessDeniedException> predicate
    )
        throws AccessDeniedException
    {
        int count = 0;
        Iterator<Resource> rscIt = rscDfn.iterateResource(accCtx);
        while (rscIt.hasNext())
        {
            Resource rsc = rscIt.next();
            if (predicate.test(rsc))
            {
                count++;
            }
        }
        return count;
    }
}
