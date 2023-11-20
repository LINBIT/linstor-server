package com.linbit.linstor.layer;

import com.linbit.linstor.layer.bcache.BCacheLayer;
import com.linbit.linstor.layer.dmsetup.cache.CacheLayer;
import com.linbit.linstor.layer.dmsetup.writecache.WritecacheLayer;
import com.linbit.linstor.layer.drbd.DrbdLayer;
import com.linbit.linstor.layer.luks.LuksLayer;
import com.linbit.linstor.layer.nvme.NvmeLayer;
import com.linbit.linstor.layer.storage.StorageLayer;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

@Singleton
public class LayerFactory
{
    private final Map<DeviceLayerKind, DeviceLayer> devLayerLookupTable;

    @Inject
    public LayerFactory(
        NvmeLayer nvmeLayer,
        DrbdLayer drbdLayer,
        StorageLayer storageLayer,
        LuksLayer luksLayer,
        WritecacheLayer writecacheLayer,
        CacheLayer cacheLayer,
        BCacheLayer bcacheLayer
    )
    {
        devLayerLookupTable = new HashMap<>();

        devLayerLookupTable.put(DeviceLayerKind.NVME, nvmeLayer);
        devLayerLookupTable.put(DeviceLayerKind.DRBD, drbdLayer);
        devLayerLookupTable.put(DeviceLayerKind.LUKS, luksLayer);
        devLayerLookupTable.put(DeviceLayerKind.STORAGE, storageLayer);
        devLayerLookupTable.put(DeviceLayerKind.WRITECACHE, writecacheLayer);
        devLayerLookupTable.put(DeviceLayerKind.CACHE, cacheLayer);
        devLayerLookupTable.put(DeviceLayerKind.BCACHE, bcacheLayer);
    }

    public DeviceLayer getDeviceLayer(DeviceLayerKind kind)
    {
        return devLayerLookupTable.get(kind);
    }

    public Stream<DeviceLayer> streamDeviceHandlers()
    {
        return devLayerLookupTable.values().stream();
    }

    public Iterator<DeviceLayer> iterateDeviceHandlers()
    {
        return devLayerLookupTable.values().iterator();
    }
}
