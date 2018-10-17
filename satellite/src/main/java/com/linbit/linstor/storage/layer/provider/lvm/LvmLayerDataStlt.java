package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmLayerData;

public class LvmLayerDataStlt implements LvmLayerData
{
    public enum Size
    {
        TOO_SMALL,
        TOO_LARGE,
        TOO_LARGE_WITHIN_TOLERANCE
    }

    boolean exists = false;
    boolean failed = false;
    long usableSize = -1L;
    long allocatedSize = -1L;
    String volumeGroup = null;
    String thinPool;
    String identifier = null;
    Size sizeState = null;

    public LvmLayerDataStlt(LvsInfo info)
    {
        this(info.volumeGroup, info.thinPool, info.identifier, info.size);
    }

    public LvmLayerDataStlt(String volumeGroupRef, String thinPoolRef, String identifierRef, long sizeRef)
    {
        volumeGroup = volumeGroupRef;
        thinPool = thinPoolRef;
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
    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }
}
