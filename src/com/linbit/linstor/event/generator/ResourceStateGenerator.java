package com.linbit.linstor.event.generator;

import com.linbit.linstor.event.ObjectIdentifier;

public interface ResourceStateGenerator
{
    Boolean generate(ObjectIdentifier objectIdentifier)
        throws Exception;
}
