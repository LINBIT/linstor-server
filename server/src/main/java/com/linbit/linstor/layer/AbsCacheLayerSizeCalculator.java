package com.linbit.linstor.layer;

import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.annotation.Nullable;

/**
 * {@link DeviceLayerKind#BCACHE}, {@link DeviceLayerKind#CACHE} and {@link DeviceLayerKind#WRITECACHE} have quite
 * similar calculations. Instead of duplicating the code, we unify the calculations with a few controlling options (i.e.
 * if a meta-cache device is required or not)
 */
public abstract class AbsCacheLayerSizeCalculator<VLM_DATA extends VlmLayerObject<?>>
    extends AbsLayerSizeCalculator
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
    private static final double ROUNDUP_INCREMENT = 0.5;

    private final long metadataSizeOnDataDevice;
    private final String propertyNamespace;
    private final CacheDeviceInfo[] cacheDevices;

    protected static class CacheDeviceInfo
    {
        private final String rscLayerSuffix;
        private final String propertyKey;
        private final String propertyDefaultValue;
        private final long minimumSizeInKib;

        public CacheDeviceInfo(
            String rscLayerSuffixRef,
            String propertyKeyRef,
            String propertyDefaultValueRef
        )
        {
            this(rscLayerSuffixRef, propertyKeyRef, propertyDefaultValueRef, 1);
        }

        public CacheDeviceInfo(
            String rscLayerSuffixRef,
            String propertyKeyRef,
            String propertyDefaultValueRef,
            long minimumSizeInKibRef
        )
        {
            rscLayerSuffix = rscLayerSuffixRef;
            propertyKey = propertyKeyRef;
            propertyDefaultValue = propertyDefaultValueRef;
            minimumSizeInKib = minimumSizeInKibRef;
        }
    }

    protected AbsCacheLayerSizeCalculator(
        AbsLayerSizeCalculatorInit initRef,
        DeviceLayerKind kindRef,
        String propertyNamespaceRef,
        long metadataSizeOnDataDeviceRef,
        CacheDeviceInfo... cacheDevicesRef
    )
    {
        super(initRef, kindRef);
        metadataSizeOnDataDevice = metadataSizeOnDataDeviceRef;
        propertyNamespace = propertyNamespaceRef;
        cacheDevices = cacheDevicesRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateAllocatedSizeFromUsableSizeImpl(VlmProviderObject<?> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        updateSize((VLM_DATA) vlmData, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void updateUsableSizeFromAllocatedSizeImpl(VlmProviderObject<?> vlmData)
        throws AccessDeniedException, DatabaseException
    {
        updateSize((VLM_DATA) vlmData, false);
    }

    private void updateSize(
        VLM_DATA vlmData,
        boolean fromUsable
    )
        throws AccessDeniedException, DatabaseException
    {
        VlmProviderObject<?> dataChildVlmData = vlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);

        boolean allCacheChildrenExists = true;
        long sumCacheAllocatedSizes;

        VlmProviderObject<?>[] cacheChildrenVlmData = new VlmProviderObject[cacheDevices.length];

        for (int cdIdx = 0; cdIdx < cacheDevices.length; cdIdx++)
        {
            CacheDeviceInfo cdi = cacheDevices[cdIdx];
            VlmProviderObject<?> cacheChildVlmData = vlmData.getChildBySuffix(cdi.rscLayerSuffix);
            cacheChildrenVlmData[cdIdx] = cacheChildVlmData;
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
            sumCacheAllocatedSizes = calculateCacheChildren(vlmData, cacheChildrenVlmData);
        }
        else
        {
            sumCacheAllocatedSizes = 0;
        }

        vlmData.setAllocatedSize(vlmData.getUsableSize() + sumCacheAllocatedSizes);
    }

    private long calculateCacheChildren(
        VLM_DATA vlmData,
        VlmProviderObject<?>[] cacheChildrenVlmData
    )
        throws AccessDeniedException, DatabaseException
    {
        long ret = 0;

        AbsVolume<?> vlm = vlmData.getVolume();
        PriorityProps prioProps = getPrioProps(vlm);

        for (int cdIdx = 0; cdIdx < cacheDevices.length; cdIdx++)
        {
            CacheDeviceInfo cdi = cacheDevices[cdIdx];
            VlmProviderObject<?> cacheChildVlmData = cacheChildrenVlmData[cdIdx];

            // null if we are above an NVMe target
            @Nullable
            String sizeStr = prioProps.getProp(cdi.propertyKey, propertyNamespace, cdi.propertyDefaultValue);

            long cacheSize;
            if (sizeStr != null)
            {
                sizeStr = sizeStr.trim();
                if (sizeStr.endsWith("%"))
                {
                    String cacheSizePercent = sizeStr.substring(0, sizeStr.length() - 1);
                    double percent = Double.parseDouble(cacheSizePercent) / 100;
                    cacheSize = Math.round(percent * vlmData.getUsableSize() + ROUNDUP_INCREMENT);
                }
                else
                {
                    cacheSize = Long.parseLong(sizeStr);
                }
                if (cacheSize < cdi.minimumSizeInKib)
                {
                    errorReporter.logDebug(
                        "%s: size %dKiB was too small. Rounded up to %dKiB",
                        kind.name(),
                        cacheSize,
                        cdi.minimumSizeInKib
                    );
                    cacheSize = cdi.minimumSizeInKib;
                }

                cacheChildVlmData.setUsableSize(cacheSize);
                updateAllocatedSizeFromUsableSize(cacheChildVlmData);

                ret += cacheSize;
            }
        }
        return ret;
    }
}
