package com.linbit.drbdmanage.api.interfaces;

import java.util.List;

import com.linbit.drbdmanage.Resource;

public interface ResourceSerializer
{
    public byte[] getChangedMessage(Resource rsc);

    public byte[] getRscDataMessage(
        int msgId,
        Resource localResource,
        List<Resource> otherResources
    );
}
