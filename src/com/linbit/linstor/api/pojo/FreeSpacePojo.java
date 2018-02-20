package com.linbit.linstor.api.pojo;

import java.util.UUID;

public class FreeSpacePojo
{
    private final UUID storPoolUuid;
    private final String storPoolName;
    private final long freeSpace;
    
    public FreeSpacePojo(
        UUID storPoolUuidRef, 
        String storPoolNameRef, 
        long freeSpaceRef
    )
    {
        storPoolUuid = storPoolUuidRef;
        storPoolName = storPoolNameRef;
        freeSpace = freeSpaceRef;
    }
    
    public UUID getStorPoolUuid()
    {
        return storPoolUuid;
    }

    public String getStorPoolName()
    {
        return storPoolName;
    }

    public long getFreeSpace()
    {
        return freeSpace;
    }
}
