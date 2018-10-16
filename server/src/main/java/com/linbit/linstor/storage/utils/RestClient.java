package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.StorageException;

import java.io.IOException;
import java.util.List;
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
        Map<Object, Object> jsonMap,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException
    {
        return execute(op, restURL, httpHeaders, JSON.std.asString(jsonMap), expectedRcs);
    }

    RestResponse<Map<String, Object>> execute(
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException;
}
