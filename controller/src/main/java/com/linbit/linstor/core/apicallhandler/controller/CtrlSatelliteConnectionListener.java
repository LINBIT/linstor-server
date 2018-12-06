package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.security.AccessDeniedException;
import reactor.core.publisher.Flux;

import java.util.Collection;
import java.util.Collections;

public interface CtrlSatelliteConnectionListener
{
    /**
     * Notifies a listener that all nodes used by a resource definition are now online.
     *
     * @return A collection of operations that can now be continued
     * @throws AccessDeniedException A privileged access context should be used by the listener, so any access denials
     * will be treated as implementation errors
     */
    default Collection<Flux<ApiCallRc>> resourceDefinitionConnected(ResourceDefinition rscDfn)
        throws AccessDeniedException
    {
        return Collections.emptySet();
    }

    /**
     * Notifies a listener that the node on which a resource is present is now online.
     *
     * @return A collection of operations that can now be continued
     * @throws AccessDeniedException A privileged access context should be used by the listener, so any access denials
     * will be treated as implementation errors
     */
    default Collection<Flux<ApiCallRc>> resourceConnected(Resource rsc)
        throws AccessDeniedException
    {
        return Collections.emptySet();
    }
}
