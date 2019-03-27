package com.linbit.linstor.core.apicallhandler.satellite.authentication;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import java.util.List;

public class AuthenticationResult
{
    private final List<DeviceLayerKind> supportedDeviceLayer;
    private final List<DeviceProviderKind> supportedDeviceProvider;

    private final ApiCallRcImpl failedApiCallRcImpl;

    public AuthenticationResult(
        List<DeviceLayerKind> supportedDeviceLayerRef,
        List<DeviceProviderKind> supportedDeviceProviderRef
    )
    {
        supportedDeviceLayer = supportedDeviceLayerRef;
        supportedDeviceProvider = supportedDeviceProviderRef;
        failedApiCallRcImpl = null;
    }

    public AuthenticationResult(ApiCallRcImpl failedApiCallRcImplRef)
    {
        supportedDeviceLayer = null;
        supportedDeviceProvider = null;
        failedApiCallRcImpl = failedApiCallRcImplRef;
    }

    public boolean isAuthenticated()
    {
        return failedApiCallRcImpl == null;
    }

    public List<DeviceLayerKind> getSupportedDeviceLayer()
    {
        return supportedDeviceLayer;
    }

    public List<DeviceProviderKind> getSupportedDeviceProvider()
    {
        return supportedDeviceProvider;
    }

    public ApiCallRcImpl getFailedApiCallRc()
    {
        return failedApiCallRcImpl;
    }
}
