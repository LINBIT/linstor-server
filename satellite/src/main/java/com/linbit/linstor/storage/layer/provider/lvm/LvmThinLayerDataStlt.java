package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.storage2.layer.data.LvmThinLayerData;

public class LvmThinLayerDataStlt extends LvmLayerDataStlt implements LvmThinLayerData
{
    public LvmThinLayerDataStlt(LvsInfo info)
    {
        super(info.volumeGroup, info.thinPool, info.identifier);
    }

    public LvmThinLayerDataStlt(String volumeGroupRef, String thinPoolRef, String identifierRef)
    {
        super(volumeGroupRef, thinPoolRef, identifierRef);
    }

    @Override
    public String getThinPool()
    {
        return thinPool;
    }
}
