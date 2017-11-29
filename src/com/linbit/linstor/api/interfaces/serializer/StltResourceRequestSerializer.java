package com.linbit.linstor.api.interfaces.serializer;

import java.util.UUID;

import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;

public interface StltResourceRequestSerializer
{
    public byte[] getRequestMessage(
        int msgId,
        UUID uuid,
        NodeName nodeName,
        ResourceName rscName
    );
}
