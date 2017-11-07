package com.linbit.drbdmanage.api.protobuf.controller.interfaces;

import java.util.List;

import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceData;

public interface ResourceDataSerializer
{
    public byte[] serialize(ResourceData rsc);

    public byte[] getChangedMessage(ResourceData rsc);

    public byte[] getRscReqResponse(
        int msgId,
        Resource localResource,
        List<Resource> otherResources
    );
}
