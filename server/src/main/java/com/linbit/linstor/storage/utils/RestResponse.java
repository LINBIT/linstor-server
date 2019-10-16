package com.linbit.linstor.storage.utils;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Map;

public interface RestResponse<T>
{
    T getData();

    int getStatusCode();

    Map<String, String> getHeaders();

    String toString(Integer... excludeExpectedRcs);

    VlmProviderObject<Resource> getVolumeData();
}
