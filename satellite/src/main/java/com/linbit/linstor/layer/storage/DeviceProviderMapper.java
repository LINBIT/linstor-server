package com.linbit.linstor.layer.storage;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.Map;

@Singleton
public class DeviceProviderMapper
{
    private final Map<DeviceProviderKind, DeviceProvider> deviceProviderMap;
    private final Node.Type localNodeType;

    @Inject
    public DeviceProviderMapper(
        StltConfig stltCfgRef,
        Map<DeviceProviderKind, DeviceProvider> deviceProviderMapRef
    )
    {
        deviceProviderMap = deviceProviderMapRef;
        localNodeType = stltCfgRef.getLocalNodeType();
    }

    public Collection<DeviceProvider> getDrivers()
    {
        return deviceProviderMap.values();
    }

    public DeviceProvider getDeviceProviderBy(StorPoolInfo storPool)
    {
        return getDeviceProviderByKind(storPool.getDeviceProviderKind());
    }

    public @Nullable DeviceProvider getDeviceProviderByKindOrNull(DeviceProviderKind deviceProviderKind)
    {
        return deviceProviderMap.get(deviceProviderKind);
    }

    public DeviceProvider getDeviceProviderByKind(DeviceProviderKind deviceProviderKind)
    {
        @Nullable DeviceProvider ret = getDeviceProviderByKindOrNull(deviceProviderKind);
        if (ret == null)
        {
            if (deviceProviderKind == DeviceProviderKind.FAIL_BECAUSE_NOT_A_VLM_PROVIDER_BUT_A_VLM_LAYER)
            {
                throw new ImplementationError("A volume from a layer was asked for its provider type");
            }
            throw new ImplementationError(deviceProviderKind.name() + " is not allowed on " + localNodeType.name());
        }
        return ret;
    }
}
