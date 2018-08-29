package com.linbit.linstor.core.apicallhandler.controller;

import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.api.ApiCallRc;
import reactor.core.publisher.Flux;

public interface CtrlSatelliteConnectionListener
{
    Flux<ApiCallRc> resourceDefinitionConnected(ResourceDefinition rscDfn);
}
