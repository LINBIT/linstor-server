package com.linbit.linstor.storage.utils;

import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface RestClient
{
    enum RestOp
    {
        GET, POST, PUT, PATCH, DELETE
    }

    <T> void addFailHandler(UnexpectedReturnCodeHandler<T> handler);

    default <T> RestResponse<T> execute(
        VlmProviderObject<Resource> vlmData,
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString,
        List<Integer> expectedRcs,
        Class<T> responseClass
    )
        throws IOException, StorageException
    {
        return execute(
            new RestHttpRequest<>(vlmData, op, restURL, httpHeaders, jsonString, expectedRcs, responseClass)
        );
    }

    <T> RestResponse<T> execute(RestHttpRequest<T> request) throws IOException, StorageException;

    class RestHttpRequest<T>
    {
        final RestOp op;
        final String restURL;
        final Map<String, String> httpHeaders;
        final String payload;
        final List<Integer> expectedRcs;
        final Class<T> responseClass;

        final VlmProviderObject<Resource> vlmData;

        RestHttpRequest(
            VlmProviderObject<Resource> vlmDataRef,
            RestOp opRef,
            String restURLRef,
            Map<String, String> httpHeadersRef,
            String payloadRef,
            List<Integer> expectedRcsRef,
            Class<T> responseClassRef
        )
        {
            vlmData = vlmDataRef;
            op = opRef;
            restURL = restURLRef;
            httpHeaders = httpHeadersRef;
            payload = payloadRef;
            expectedRcs = expectedRcsRef;
            responseClass = responseClassRef;
        }
    }

    @FunctionalInterface
    interface UnexpectedReturnCodeHandler<T>
    {
        void handle(RestResponse<T> response);
    }

    void setRetryCountOnStatusCode(int statusCode, int retryCount);

    void setRetryDelayOnStatusCode(int statusCode, long retryDelay);
}
