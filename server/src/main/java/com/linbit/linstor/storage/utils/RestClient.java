package com.linbit.linstor.storage.utils;

import com.linbit.linstor.annotation.Nullable;
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
        @Nullable VlmProviderObject<Resource> vlmData,
        RestOp op,
        String restURL,
        @Nullable Map<String, String> httpHeaders,
        @Nullable String jsonString,
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
        final @Nullable Map<String, String> httpHeaders;
        final @Nullable String payload;
        final List<Integer> expectedRcs;
        final @Nullable Class<T> responseClass;

        final @Nullable VlmProviderObject<Resource> vlmData;

        RestHttpRequest(
            @Nullable VlmProviderObject<Resource> vlmDataRef,
            RestOp opRef,
            String restURLRef,
            @Nullable Map<String, String> httpHeadersRef,
            @Nullable String payloadRef,
            List<Integer> expectedRcsRef,
            @Nullable Class<T> responseClassRef
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
