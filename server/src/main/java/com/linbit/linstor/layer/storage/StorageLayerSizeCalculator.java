package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerSizeCalculator;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class StorageLayerSizeCalculator extends AbsLayerSizeCalculator
{
    @Inject
    public StorageLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.STORAGE);
    }

    @Override
    protected void updateAllocatedSizeFromUsableSizeImpl(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        updateGrossSize(vlmDataRef);
    }

    @Override
    protected void updateUsableSizeFromAllocatedSizeImpl(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        // just copy (for now) usableSize = allocateSize and let the DeviceProviders recalculate the allocatedSize
        vlmDataRef.setUsableSize(vlmDataRef.getAllocatedSize());
        updateGrossSize(vlmDataRef);
    }

    public void updateGrossSize(VlmProviderObject<?> vlmDataRef) throws AccessDeniedException
    {
        if (!vlmDataRef.getProviderKind().equals(DeviceProviderKind.DISKLESS))
        {
            /*
             * Check if we need to round up to the next extent-size. For this we use the properties from the storage
             * pool and the volumedefinition to get the allocation granularity.
             *
             * IMPORTANT: The storage pool property needs to be set before calling this method
             */
            StorPool sp = vlmDataRef.getStorPool();

            /*
             * DO NOT USE PriorityProps here!
             * We need to take the higher allocation granularity here - by VALUE not by PRIORIRTY
             */
            long spAllocGran = getAllocationGranularity(sp.getProps(sysCtx));
            long vlmDfnAllocGran = getAllocationGranularity(
                vlmDataRef.getVolume().getVolumeDefinition().getProps(sysCtx)
            );
            long maxAllocGran = Math.max(vlmDfnAllocGran, spAllocGran);

            long volumeSize = vlmDataRef.getUsableSize();

            if (volumeSize % maxAllocGran != 0)
            {
                // round up to the next extent
                long origSize = volumeSize;
                volumeSize = ((volumeSize / maxAllocGran) + 1) * maxAllocGran;
                final String device = vlmDataRef.getDevicePath() == null ?
                    vlmDataRef.getRscLayerObject().getSuffixedResourceName() + "/" + vlmDataRef.getVlmNr().value :
                    vlmDataRef.getDevicePath();
                errorReporter.logInfo(
                    String.format(
                        "Aligning %s size from %d KiB to %d KiB to be a multiple of extent size %d KiB (from %s)",
                        device,
                        origSize,
                        volumeSize,
                        maxAllocGran,
                        maxAllocGran == spAllocGran ? "Storage Pool" : "Volume Definition"
                    )
                );
            }

            // usable size was just updated (set) by the layer above us. copy that, so we can
            // update it again with the actual usable size when we are finished
            vlmDataRef.setExpectedSize(volumeSize);
            try
            {
                vlmDataRef.setUsableSize(volumeSize);
            }
            catch (DatabaseException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    private long getAllocationGranularity(ReadOnlyProps propsRef)
    {
        String allocGran = propsRef.getProp(
            InternalApiConsts.ALLOCATION_GRANULARITY,
            StorageConstants.NAMESPACE_INTERNAL
        );
        long ret;
        if (allocGran == null)
        {
            ret = 1; // old vlmDfn, value has not yet been recalcuated by controller
        }
        else
        {
            ret = Long.parseLong(allocGran);
        }
        return ret;
    }
}
