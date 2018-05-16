package com.linbit.linstor.event.generator;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.event.ObjectIdentifier;

public interface ResourceDeploymentStateGenerator
{
    ApiCallRc generate(ObjectIdentifier objectIdentifier)
        throws Exception;

    void clear(ObjectIdentifier objectIdentifier)
        throws Exception;
}
