package com.linbit.linstor.layer.nvme;

import com.linbit.linstor.layer.AbsNoopSizeCalculator;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class NvmeLayerSizeCalculator extends AbsNoopSizeCalculator
{
    @Inject
    public NvmeLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.NVME);
    }
}
