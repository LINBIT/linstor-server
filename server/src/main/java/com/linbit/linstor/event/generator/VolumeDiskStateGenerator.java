package com.linbit.linstor.event.generator;

import com.linbit.linstor.event.ObjectIdentifier;

public interface VolumeDiskStateGenerator
{
    String generate(ObjectIdentifier objectIdentifier)
        throws Exception;
}
