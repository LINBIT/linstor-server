package com.linbit.linstor.storage.utils;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

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

    void addFailHandler(UnexpectedReturnCodeHandler handler);

    default RestResponse<Map<String, Object>> execute(
        VlmProviderObject<Resource> vlmData,
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        Map<Object, Object> jsonMap,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException
    {
        return execute(
            new RestHttpRequest(
                vlmData,
                op,
                restURL,
                httpHeaders,
                JSON.std.asString(jsonMap),
                expectedRcs
            )
        );
    }

    default RestResponse<Map<String, Object>> execute(
        VlmProviderObject<Resource> vlmData,
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException
    {
        return execute(
            new RestHttpRequest(vlmData, op, restURL, httpHeaders, jsonString, expectedRcs)
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

        final VlmProviderObject<Resource> vlmData;

        RestHttpRequest(
            VlmProviderObject<Resource> vlmDataRef,
            RestOp opRef,
            String restURLRef,
            Map<String, String> httpHeadersRef,
            String payloadRef,
            List<Integer> expectedRcsRef
        )
        {
            vlmData = vlmDataRef;
            op = opRef;
            restURL = restURLRef;
            httpHeaders = httpHeadersRef;
            payload = payloadRef;
            expectedRcs = expectedRcsRef;
        }
    }

    @FunctionalInterface
    interface UnexpectedReturnCodeHandler
    {
        void handle(RestResponse<Map<String, Object>> response);
    }

    void setRetryCountOnStatusCode(int statusCode, int retryCount);

    void setRetryDelayOnStatusCode(int statusCode, long retryDelay);
}
