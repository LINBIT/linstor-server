package com.linbit.linstor.storage.utils;

import java.util.Map;

public interface RestResponse<T>
{
    T getData();

    int getStatusCode();

    Map<String, String> getHeaders();
}
