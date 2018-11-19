package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.storage2.layer.data.ZfsLayerData;

public class ZfsLayerDataStlt implements ZfsLayerData
{
    boolean exists = false;
    boolean failed = false;
    long usableSize = -1L;
    long allocatedSize = -1L;
    String zpool = null;
    String identifier = null;
    Size sizeState = null;

    public ZfsLayerDataStlt(ZfsInfo info)
    {
        this(info.poolName, info.identifier, info.size);
    }

    public ZfsLayerDataStlt(String zPoolRef, String identifierRef, long sizeRef)
    {
        zpool = zPoolRef;
        identifier = identifierRef;
        usableSize = sizeRef;
        allocatedSize = sizeRef;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    @Override
    public String getZPool()
    {
        return zpool;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }
}
