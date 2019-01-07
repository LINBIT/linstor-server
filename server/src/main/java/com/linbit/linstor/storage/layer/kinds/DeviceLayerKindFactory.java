package com.linbit.linstor.storage.layer.kinds;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class DeviceLayerKindFactory
{
    public static final String DRBD_KIND = DrbdLayerKind.class.getSimpleName();
    public static final String DRBD_PROXY_KIND = DrbdProxyLayerKind.class.getSimpleName();
    public static final String STORAGE_KIND = StorageLayerKind.class.getSimpleName();

    private static final Map<String, DeviceLayerKind> FACTORIES;

    static
    {
        Map<String, DeviceLayerKind> map = new TreeMap<>();
        FACTORIES = Collections.unmodifiableMap(map);
        map.put(DRBD_KIND, new DrbdLayerKind());
        map.put(DRBD_PROXY_KIND, new DrbdProxyLayerKind());
        map.put(STORAGE_KIND, new StorageLayerKind());
    }

    public static DeviceLayerKind getKind(String simpleName)
    {
        DeviceLayerKind ret = FACTORIES.get(simpleName);
        if (ret == null)
        {
            throw new IllegalArgumentException("Unknown storage driver " + simpleName);
        }
        return ret;
    }

    public static Collection<DeviceLayerKind> getKinds()
    {
        return FACTORIES.values();
    }

    public static Map<String, DeviceLayerKind> getFactories()
    {
        return FACTORIES;
    }

    private DeviceLayerKindFactory()
    {
    }
}
