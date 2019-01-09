package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.utils.RemoveAfterDevMgrRework;

import java.util.Map;

public interface RestResponse<T>
{
    T getData();

    int getStatusCode();

    Map<String, String> getHeaders();

    @RemoveAfterDevMgrRework
    String getLinstorVlmId();

    String toString(Integer... excludeExpectedRcs);

    VlmProviderObject getVolumeData();
}
