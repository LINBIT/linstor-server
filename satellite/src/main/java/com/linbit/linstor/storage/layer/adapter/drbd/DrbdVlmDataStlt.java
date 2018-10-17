package com.linbit.linstor.storage.layer.adapter.drbd;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.storage2.layer.data.DrbdVlmData;

public class DrbdVlmDataStlt implements DrbdVlmData
{
    boolean exists;
    boolean failed;
    String metaDiskPath;
    String diskState;

    transient long allocatedSize;
    transient long usableSize;
    transient short peerSlots;
    transient int alStripes;
    transient long alStripeSize;
    transient boolean hasMetaData;
    transient boolean checkMetaData;
    transient boolean metaDataIsNew;
    transient boolean hasDisk;

    public DrbdVlmDataStlt()
    {
        exists = false;
        failed = false;
        metaDiskPath = null;

        allocatedSize = -1;
        usableSize = -1;
        peerSlots = InternalApiConsts.DEFAULT_PEER_SLOTS;
        alStripes = -1;
        alStripeSize = -1L;

        checkMetaData = true;
        metaDataIsNew = false;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
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
    public String getMetaDiskPath()
    {
        return metaDiskPath;
    }

    @Override
    public String getDiskState()
    {
        return diskState;
    }
}
