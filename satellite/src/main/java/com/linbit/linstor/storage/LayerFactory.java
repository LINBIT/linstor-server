package com.linbit.linstor.storage;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.adapter.dmsetup.WritecacheLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdLayer;
import com.linbit.linstor.storage.layer.adapter.luks.LuksLayer;
import com.linbit.linstor.storage.layer.adapter.nvme.NvmeLayer;
import com.linbit.linstor.storage.layer.provider.StorageLayer;

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
        WritecacheLayer writecacheLayer
    )
    {
        devLayerLookupTable = new HashMap<>();

        devLayerLookupTable.put(DeviceLayerKind.NVME, nvmeLayer);
        devLayerLookupTable.put(DeviceLayerKind.DRBD, drbdLayer);
        devLayerLookupTable.put(DeviceLayerKind.LUKS, luksLayer);
        devLayerLookupTable.put(DeviceLayerKind.STORAGE, storageLayer);
        devLayerLookupTable.put(DeviceLayerKind.WRITECACHE, writecacheLayer);
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
