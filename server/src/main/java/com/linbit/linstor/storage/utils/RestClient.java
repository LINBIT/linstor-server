package com.linbit.linstor.storage.utils;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.jr.ob.JSON;

public interface RestClient
{
    enum RestOp
    {
        GET, POST, PUT, PATCH, DELETE
    }

    default RestResponse<Map<String, Object>> execute(
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        Map<Object, Object> jsonMap
    )
        throws IOException
    {
        return execute(op, restURL, httpHeaders, JSON.std.asString(jsonMap));
    }

    RestResponse<Map<String, Object>> execute(
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString
    )
        throws IOException;
}
