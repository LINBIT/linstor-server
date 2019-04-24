package com.linbit.linstor.core.apicallhandler.satellite.authentication;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import java.util.List;

public class AuthenticationResult
{
    private final List<DeviceLayerKind> supportedDeviceLayer;
    private final List<DeviceProviderKind> supportedDeviceProvider;

    private final boolean authenticated;

    private final ApiCallRcImpl apiCallRcImpl;

    public AuthenticationResult(
        List<DeviceLayerKind> supportedDeviceLayerRef,
        List<DeviceProviderKind> supportedDeviceProviderRef,
        ApiCallRcImpl apiCallRcImplRef
    )
    {
        supportedDeviceLayer = supportedDeviceLayerRef;
        supportedDeviceProvider = supportedDeviceProviderRef;
        apiCallRcImpl = apiCallRcImplRef;
        authenticated = true;
    }

    public AuthenticationResult(ApiCallRcImpl failedApiCallRcImplRef)
    {
        supportedDeviceLayer = null;
        supportedDeviceProvider = null;
        apiCallRcImpl = failedApiCallRcImplRef;
        authenticated = false;
    }

    public boolean isAuthenticated()
    {
        return authenticated;
    }

    public List<DeviceLayerKind> getSupportedDeviceLayer()
    {
        return supportedDeviceLayer;
    }

    public List<DeviceProviderKind> getSupportedDeviceProvider()
    {
        return supportedDeviceProvider;
    }

    public ApiCallRcImpl getApiCallRc()
    {
        return apiCallRcImpl;
    }
}
