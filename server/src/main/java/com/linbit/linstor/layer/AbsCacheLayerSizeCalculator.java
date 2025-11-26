package com.linbit.linstor.layer;

import com.linbit.exceptions.InvalidSizeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link DeviceLayerKind#BCACHE}, {@link DeviceLayerKind#CACHE} and {@link DeviceLayerKind#WRITECACHE} have quite
 * similar calculations. Instead of duplicating the code, we unify the calculations with a few controlling options (i.e.
 * if a meta-cache device is required or not)
 */
public abstract class AbsCacheLayerSizeCalculator<VLM_DATA extends VlmLayerObject<?>>
    extends AbsLayerSizeCalculator<VLM_DATA>
{
    /**
     * This is more of a hack. We want to round up a double into a long. {@link Math#ceil(double)} however returns us a
     * double instead of a long, which could result in returning 19.99999 instead of 20. Casting that to long will
     * incorrectly result in 19.
     *
     * {@link Math#round(double)} does return a long, but does not round up (i.e. 19.4 would become 19).
     * Incrementing the number before the round with 0.5 allows us to use the {@link Math#round(double)} as a "round up"
     * method.
     */
    protected static final double ROUNDUP_INCREMENT = 0.5;

    private final long metadataSizeOnDataDevice;
    private final String propertyNamespace;
    private final List<CacheDeviceCalculator> cacheDevicesSizeCalculators;

    protected interface SpecialDeviceSizeCalculator<VLM_DATA extends VlmLayerObject<?>>
    {
        long calculate(VLM_DATA vlmDataRef, VlmProviderObject<?> cacheChildVlmDataRef)
            throws AccessDeniedException, DatabaseException, InvalidSizeException;
    }

    protected class CacheDeviceCalculator
    {
        private final String rscLayerSuffix;
        private final SpecialDeviceSizeCalculator<VLM_DATA> specialCalc;

        public CacheDeviceCalculator(String rscLayerSuffixRef, SpecialDeviceSizeCalculator<VLM_DATA> calcFuncRef)
        {
            rscLayerSuffix = rscLayerSuffixRef;
            specialCalc = calcFuncRef;
        }
    }

    protected AbsCacheLayerSizeCalculator(
        AbsLayerSizeCalculatorInit initRef,
        DeviceLayerKind kindRef,
        String propertyNamespaceRef,
        long metadataSizeOnDataDeviceRef
    )
    {
        super(initRef, kindRef);
        metadataSizeOnDataDevice = metadataSizeOnDataDeviceRef;
        propertyNamespace = propertyNamespaceRef;

        cacheDevicesSizeCalculators = new ArrayList<>();
    }

    protected void registerChildSizeCalculator(
        String rscLayerSuffixRef,
        String propertyKeyRef,
        String propertyDefaultValueRef
    )
    {
        registerChildSizeCalculator(rscLayerSuffixRef, propertyKeyRef, propertyDefaultValueRef, 1);
    }

    protected void registerChildSizeCalculator(
        String rscLayerSuffixRef,
        String propertyKeyRef,
        String propertyDefaultValueRef,
        long minimumSizeInKibRef
    )
    {
        registerChildSizeCalculator(
            rscLayerSuffixRef,
            (vlmData, cacheChildVlmData) -> getCacheSize(
                cacheChildVlmData,
                vlmData.getUsableSize(),
                propertyKeyRef,
                propertyDefaultValueRef,
                minimumSizeInKibRef
            )
        );
    }

    protected void registerChildSizeCalculator(
        String rscLayerSuffixRef,
        SpecialDeviceSizeCalculator<VLM_DATA> specialDeviceSizeCalculatorRef
    )
    {
        this.cacheDevicesSizeCalculators.add(
            new CacheDeviceCalculator(rscLayerSuffixRef, specialDeviceSizeCalculatorRef)
        );
    }

    @Override
    public void updateAllocatedSizeFromUsableSizeImpl(VLM_DATA vlmData)
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        updateSize(vlmData, true);
    }

    @Override
    public void updateUsableSizeFromAllocatedSizeImpl(VLM_DATA vlmData)
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        updateSize(vlmData, false);
    }

    private void updateSize(
        VLM_DATA vlmData,
        boolean fromUsable
    )
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        VlmProviderObject<?> dataChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);

        boolean allCacheChildrenExists = true;
        long sumCacheAllocatedSizes;


        for (CacheDeviceCalculator calc : cacheDevicesSizeCalculators)
        {
            VlmProviderObject<?> cacheChildVlmData = vlmData.getChildBySuffix(calc.rscLayerSuffix);
            if (cacheChildVlmData == null)
            {
                allCacheChildrenExists = false;
            }
        }

        if (fromUsable)
        {
            dataChildVlmData.setUsableSize(vlmData.getUsableSize() + metadataSizeOnDataDevice);
            updateAllocatedSizeFromUsableSize(dataChildVlmData);
        }
        else
        {
            dataChildVlmData.setAllocatedSize(vlmData.getAllocatedSize() + metadataSizeOnDataDevice);
            updateUsableSizeFromAllocatedSize(dataChildVlmData);

            // this should be done before using calcSize since that method is based on our usable size
            vlmData.setUsableSize(vlmData.getAllocatedSize());
        }

        if (allCacheChildrenExists)
        {
            sumCacheAllocatedSizes = calculateCacheChildren(vlmData);
        }
        else
        {
            sumCacheAllocatedSizes = 0;
        }

        vlmData.setAllocatedSize(vlmData.getUsableSize() + sumCacheAllocatedSizes);
    }

    private long calculateCacheChildren(VLM_DATA vlmData)
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        long ret = 0;
        for (CacheDeviceCalculator calc : cacheDevicesSizeCalculators)
        {
            VlmProviderObject<?> cacheChildVlmData = vlmData.getChildBySuffix(calc.rscLayerSuffix);
            ret += calc.specialCalc.calculate(vlmData, cacheChildVlmData);
        }
        return ret;
    }

    protected long getCacheSize(
        VlmProviderObject<?> cacheChildVlmDataRef,
        long vlmDataUsableSize,
        String propertyKeyRef,
        String propertyDefaultValueRef,
        long minimumSizeInKibRef
    )
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        long ret = 0;

        AbsVolume<?> vlm = cacheChildVlmDataRef.getVolume();
        PriorityProps prioProps = getPrioProps(vlm);

        // null if we are above an NVMe target
        @Nullable String sizeStr = prioProps.getProp(propertyKeyRef, propertyNamespace, propertyDefaultValueRef);

        long cacheSize;
        if (sizeStr != null)
        {
            sizeStr = sizeStr.trim();
            if (sizeStr.endsWith("%"))
            {
                String cacheSizePercent = sizeStr.substring(0, sizeStr.length() - 1);
                double percent = Double.parseDouble(cacheSizePercent) / 100;
                cacheSize = Math.round(percent * vlmDataUsableSize + ROUNDUP_INCREMENT);
            }
            else
            {
                cacheSize = Long.parseLong(sizeStr);
            }
            if (cacheSize < minimumSizeInKibRef)
            {
                errorReporter.logDebug(
                    "%s: size %dKiB was too small. Rounded up to %dKiB",
                    kind.name(),
                    cacheSize,
                    minimumSizeInKibRef
                );
                cacheSize = minimumSizeInKibRef;
            }

            cacheChildVlmDataRef.setUsableSize(cacheSize);
            updateAllocatedSizeFromUsableSize(cacheChildVlmDataRef);
        }
        return ret;
    }
}
