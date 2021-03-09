package com.linbit.linstor.core.apicallhandler.satellite.authentication;

import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;

import java.util.Collection;

public class AuthenticationResult
{
    private final Collection<ExtToolsInfo> extToolsInfoList;

    private final boolean authenticated;

    private final ApiCallRcImpl apiCallRcImpl;

    public AuthenticationResult(
        Collection<ExtToolsInfo> extToolsInfoListRef,
        ApiCallRcImpl apiCallRcImplRef
    )
    {
        extToolsInfoList = extToolsInfoListRef;
        apiCallRcImpl = apiCallRcImplRef;
        authenticated = true;
    }

    public AuthenticationResult(ApiCallRcImpl failedApiCallRcImplRef)
    {
        extToolsInfoList = null;
        apiCallRcImpl = failedApiCallRcImplRef;
        authenticated = false;
    }

    public boolean isAuthenticated()
    {
        return authenticated;
    }

    public Collection<ExtToolsInfo> getExternalToolsInfoList()
    {
        return extToolsInfoList;
    }

    public ApiCallRcImpl getApiCallRc()
    {
        return apiCallRcImpl;
    }
}
