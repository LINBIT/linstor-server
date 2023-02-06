package com.linbit.linstor.core.apicallhandler.utils;

import com.linbit.linstor.core.CoreModule.ResourceDefinitionMap;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.VolumeGroup;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;

public class LinstorIteratorUtils
{
    private LinstorIteratorUtils()
    {
        // utility class
    }

    public static <T> ArrayList<T> foreachRsc(
        AccessContext accCtx,
        Node nodeRef,
        Function<Resource, T> fct
    )
        throws AccessDeniedException
    {
        ArrayList<T> ret = new ArrayList<>();
        Iterator<Resource> rscIt = nodeRef.iterateResources(accCtx);
        while (rscIt.hasNext())
        {
            ret.add(fct.apply(rscIt.next()));
        }
        return ret;
    }

    public static <T> Collection<T> foreachRscDfn(
        AccessContext accCtxRef,
        VolumeGroup vlmGrpRef,
        Function<ResourceDefinition, T> fct
    )
        throws AccessDeniedException
    {
        return foreachRscDfn(accCtxRef, vlmGrpRef.getResourceGroup(), fct);
    }

    public static <T> ArrayList<T> foreachRscDfn(
        AccessContext accCtx,
        ResourceGroup rscGrpRef,
        Function<ResourceDefinition, T> fct
    )
        throws AccessDeniedException
    {
        ArrayList<T> ret = new ArrayList<>();
        for (ResourceDefinition rscDfn : getRscDfns(accCtx, rscGrpRef))
        {
            ret.add(fct.apply(rscDfn));
        }
        return ret;
    }

    public static <T> ArrayList<T> foreachRscDfn(
        ResourceDefinitionMap rscDfnMapRef,
        Function<ResourceDefinition, T> fct
    )
    {
        ArrayList<T> ret = new ArrayList<>();
        for (ResourceDefinition rscDfn : rscDfnMapRef.values())
        {
            ret.add(fct.apply(rscDfn));
        }
        return ret;
    }

    public static Collection<ResourceDefinition> getRscDfns(AccessContext accCtxRef, ResourceGroup rscGrpRef)
        throws AccessDeniedException
    {
        return rscGrpRef.getRscDfns(accCtxRef);
    }

    public static Collection<ResourceDefinition> getRscDfns(AccessContext accCtxRef, VolumeGroup vlmGrpRef)
        throws AccessDeniedException
    {
        return getRscDfns(accCtxRef, vlmGrpRef.getResourceGroup());
    }

}
