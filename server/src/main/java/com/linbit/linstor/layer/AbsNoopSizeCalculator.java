package com.linbit.linstor.layer;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

public abstract class AbsNoopSizeCalculator extends AbsLayerSizeCalculator<VlmProviderObject<?>>
{

    protected AbsNoopSizeCalculator(AbsLayerSizeCalculatorInit initRef, DeviceLayerKind kindRef)
    {
        super(initRef, kindRef);
    }

    @Override
    protected void updateAllocatedSizeFromUsableSizeImpl(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // basically no-op. gross == net
        long size = vlmDataRef.getUsableSize();
        vlmDataRef.setAllocatedSize(size);

        if (vlmDataRef instanceof VlmLayerObject)
        {
            VlmProviderObject<?> childVlmData = ((VlmLayerObject<?>) vlmDataRef).getSingleChild();
            if (childVlmData != null)
            {
                childVlmData.setUsableSize(size);
                updateAllocatedSizeFromUsableSize(childVlmData);
            }
        }
    }

    @Override
    protected void updateUsableSizeFromAllocatedSizeImpl(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // basically no-op. gross == net for NVMe
        long size = vlmDataRef.getAllocatedSize();
        vlmDataRef.setUsableSize(size);

        if (vlmDataRef instanceof VlmLayerObject)
        {
            VlmProviderObject<?> childVlmData = ((VlmLayerObject<?>) vlmDataRef).getSingleChild();
            if (childVlmData != null)
            {
                childVlmData.setAllocatedSize(size);
                updateUsableSizeFromAllocatedSize(childVlmData);
            }
        }
    }
}
