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
        return execute(
            new RestHttpRequest(op, restURL, httpHeaders, JSON.std.asString(jsonMap), expectedRcs)
        );
    }

    default RestResponse<Map<String, Object>> execute(
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException
    {
        return execute(
            new RestHttpRequest(op, restURL, httpHeaders, jsonString, expectedRcs)
        );
    }

    RestResponse<Map<String, Object>> execute(RestHttpRequest request) throws IOException, StorageException;

    class RestHttpRequest
    {
        final RestOp op;
        final String restURL;
        final Map<String, String> httpHeaders;
        final String payload;
        final List<Integer> expectedRcs;

        RestHttpRequest(
            RestOp opRef,
            String restURLRef,
            Map<String, String> httpHeadersRef,
            String payloadRef,
            List<Integer> expectedRcsRef
        )
        {
            op = opRef;
            restURL = restURLRef;
            httpHeaders = httpHeadersRef;
            payload = payloadRef;
            expectedRcs = expectedRcsRef;
        }
    }
}
