package com.linbit.linstor.core.apicallhandler.controller.utils;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.CtrlSnapshotDeleteApiCallHandler;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.tasks.AutoSnapshotTask;
import com.linbit.utils.ExceptionThrowingPredicate;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import reactor.core.publisher.Flux;

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

    public static Flux<ApiCallRc> handleAutoSnapProps(
        AutoSnapshotTask autoSnapshotTaskRef,
        CtrlSnapshotDeleteApiCallHandler ctrlSnapDeleteHandlerRef,
        Map<String, String> overrideProps,
        Set<String> deletePropKeys,
        Set<String> deletedNamespaces,
        Collection<ResourceDefinition> affectedRscDfnListRef,
        AccessContext accCtxRef,
        Props ctrlProps
    )
        throws AccessDeniedException
    {
        Flux<ApiCallRc> retFlux = Flux.empty();
        String autoSnapKey = ApiConsts.NAMESPC_AUTO_SNAPSHOT + "/" + ApiConsts.KEY_RUN_EVERY;
        String autoSnapVal = overrideProps.get(autoSnapKey);
        boolean namespaceDeleted = deletedNamespaces.contains(ApiConsts.NAMESPC_AUTO_SNAPSHOT);
        if (autoSnapVal != null)
        {
            for (ResourceDefinition rscDfn : affectedRscDfnListRef)
            {
                PriorityProps prioProps = new PriorityProps(
                    rscDfn.getProps(accCtxRef),
                    rscDfn.getResourceGroup().getProps(accCtxRef),
                    ctrlProps
                );
                String prioPropVal = prioProps.getProp(autoSnapKey);
                retFlux = retFlux.concatWith(
                    autoSnapshotTaskRef.addAutoSnapshotting(
                        rscDfn.getName().displayValue,
                        Long.parseLong(prioPropVal)
                    )
                );
            }
        }
        else
        {
            if (deletePropKeys.contains(autoSnapKey) || namespaceDeleted)
            {
                for (ResourceDefinition rscDfn : affectedRscDfnListRef)
                {
                    PriorityProps prioProps = new PriorityProps(
                        rscDfn.getProps(accCtxRef),
                        rscDfn.getResourceGroup().getProps(accCtxRef),
                        ctrlProps
                    );
                    String prioPropVal = prioProps.getProp(autoSnapKey);
                    if (prioPropVal == null)
                    {
                        autoSnapshotTaskRef.removeAutoSnapshotting(rscDfn.getName().displayValue);
                    }
                    else
                    {
                        retFlux = retFlux.concatWith(
                            autoSnapshotTaskRef.addAutoSnapshotting(
                                rscDfn.getName().displayValue,
                                Long.parseLong(prioPropVal)
                            )
                        );
                    }
                }
            }
        }

        String autoSnapKeepKey = ApiConsts.NAMESPC_AUTO_SNAPSHOT + "/" + ApiConsts.KEY_KEEP;
        if (overrideProps.containsKey(autoSnapKeepKey) || deletePropKeys.contains(autoSnapKeepKey))
        {
            for (ResourceDefinition rscDfn : affectedRscDfnListRef)
            {
                retFlux = retFlux.concatWith(ctrlSnapDeleteHandlerRef.cleanupOldAutoSnapshots(rscDfn));
            }
        }
        return retFlux;
    }
}
