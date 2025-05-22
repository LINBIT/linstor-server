package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.linstor.layer.bcache.BCacheLayerSizeCalculator;
import com.linbit.linstor.layer.cache.CacheLayerSizeCalculator;
import com.linbit.linstor.layer.drbd.DrbdLayerSizeCalculator;
import com.linbit.linstor.layer.luks.LuksLayerSizeCalculator;
import com.linbit.linstor.layer.nvme.NvmeLayerSizeCalculator;
import com.linbit.linstor.layer.storage.StorageLayerSizeCalculator;
import com.linbit.linstor.layer.writecache.WritecacheLayerSizeCalculator;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.StringUtils;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;

public class LayerSizeCalculatorModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        Map<DeviceLayerKind, Class<? extends AbsLayerSizeCalculator<?>>> allSizeCalculators = new EnumMap<>(
            DeviceLayerKind.class
        );
        allSizeCalculators.put(DeviceLayerKind.DRBD, DrbdLayerSizeCalculator.class);
        allSizeCalculators.put(DeviceLayerKind.BCACHE, BCacheLayerSizeCalculator.class);
        allSizeCalculators.put(DeviceLayerKind.CACHE, CacheLayerSizeCalculator.class);
        allSizeCalculators.put(DeviceLayerKind.LUKS, LuksLayerSizeCalculator.class);
        allSizeCalculators.put(DeviceLayerKind.NVME, NvmeLayerSizeCalculator.class);
        allSizeCalculators.put(DeviceLayerKind.WRITECACHE, WritecacheLayerSizeCalculator.class);
        allSizeCalculators.put(DeviceLayerKind.STORAGE, StorageLayerSizeCalculator.class);

        // sanity check
        List<DeviceLayerKind> missingKinds = new ArrayList<>();
        for (DeviceLayerKind kind : DeviceLayerKind.values())
        {
            if (!allSizeCalculators.containsKey(kind))
            {
                missingKinds.add(kind);
            }
        }
        if (!missingKinds.isEmpty())
        {
            throw new ImplementationError(
                "The following DeviceLayerKinds do not have a LayerSizeCalculator implemented: " + StringUtils.join(
                    missingKinds,
                    ", "
                )
            );
        }

        MapBinder<DeviceLayerKind, AbsLayerSizeCalculator<?>> absLayerSizeCalcMapBinder = MapBinder.newMapBinder(
            binder(),
            new TypeLiteral<DeviceLayerKind>()
            {
            },
            new TypeLiteral<AbsLayerSizeCalculator<?>>()
            {
            }
        );
        for (Entry<DeviceLayerKind, Class<? extends AbsLayerSizeCalculator<?>>> entry : allSizeCalculators.entrySet())
        {
            absLayerSizeCalcMapBinder.addBinding(entry.getKey()).to(entry.getValue());
        }
    }
}
