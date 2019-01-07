package com.linbit.linstor.storage;

import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.adapter.DefaultLayer;
import com.linbit.linstor.storage.layer.adapter.cryptsetup.CryptSetupLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdLayer;
import com.linbit.linstor.storage.layer.kinds.CryptSetupLayerKind;
import com.linbit.linstor.storage.layer.kinds.DefaultLayerKind;
import com.linbit.linstor.storage.layer.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.layer.kinds.DrbdLayerKind;
import com.linbit.linstor.storage.layer.kinds.StorageLayerKind;
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
    private final Map<Class<? extends DeviceLayerKind>, ResourceLayer> devLayerLookupTable;

    @Inject
    public LayerFactory(
        DefaultLayer dfltLayer,
        DrbdLayer drbdLayer,
        StorageLayer storageLayer,
        CryptSetupLayer cryptSetupLayer
    )
    {
        devLayerLookupTable = new HashMap<>();

        devLayerLookupTable.put(DefaultLayerKind.class, dfltLayer);
        devLayerLookupTable.put(DrbdLayerKind.class, drbdLayer);
        devLayerLookupTable.put(StorageLayerKind.class, storageLayer);
        devLayerLookupTable.put(CryptSetupLayerKind.class, cryptSetupLayer);
    }

    public ResourceLayer getDeviceLayer(Class<? extends DeviceLayerKind> kindClass)
    {
        return devLayerLookupTable.get(kindClass);
    }

    public Stream<ResourceLayer> streamDeviceHandlers()
    {
        return devLayerLookupTable.values().stream();
    }

    public Iterator<ResourceLayer> iterateDeviceHandlers()
    {
        return devLayerLookupTable.values().iterator();
    }
}
