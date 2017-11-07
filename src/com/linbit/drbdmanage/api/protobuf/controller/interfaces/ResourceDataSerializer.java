package com.linbit.drbdmanage.api.protobuf.controller.interfaces;

import com.linbit.drbdmanage.ResourceData;

public interface ResourceDataSerializer
{
    public byte[] serialize(ResourceData rsc);

    public byte[] getChangedMessage(ResourceData rsc);
}
