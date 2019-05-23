package com.linbit.linstor;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.transaction.TransactionObject;

public interface VolumeDefinitionGroup
    extends TransactionObject, Comparable<VolumeDefinitionGroup>, DbgInstanceUuid
{
    VolumeNumber getVolumeNumber();

    long getSize();

    Props getProps();
}
