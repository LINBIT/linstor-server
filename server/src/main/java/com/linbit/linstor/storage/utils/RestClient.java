package com.linbit.linstor.storage.utils;

import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.utils.RemoveAfterDevMgrRework;

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
        @RemoveAfterDevMgrRework String linstorVlmId,
        VlmProviderObject vlmData,
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
                linstorVlmId,
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
        @RemoveAfterDevMgrRework String linstorVlmId,
        VlmProviderObject vlmData,
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException
    {
        return execute(
            new RestHttpRequest(linstorVlmId, vlmData, op, restURL, httpHeaders, jsonString, expectedRcs)
        );
    }

    RestResponse<Map<String, Object>> execute(RestHttpRequest request) throws IOException, StorageException;

    class RestHttpRequest
    {
        @RemoveAfterDevMgrRework
        final String linstorVlmId;

        final RestOp op;
        final String restURL;
        final Map<String, String> httpHeaders;
        final String payload;
        final List<Integer> expectedRcs;

        final VlmProviderObject vlmData;

        RestHttpRequest(
            @RemoveAfterDevMgrRework String linstorVlmIdRef,
            VlmProviderObject vlmDataRef,
            RestOp opRef,
            String restURLRef,
            Map<String, String> httpHeadersRef,
            String payloadRef,
            List<Integer> expectedRcsRef
        )
        {
            linstorVlmId = linstorVlmIdRef;
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
