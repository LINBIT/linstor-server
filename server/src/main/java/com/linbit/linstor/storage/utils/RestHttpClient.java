package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jr.ob.JSON;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

public class RestHttpClient implements RestClient
{
    public static final ObjectMapper OBJECT_MAPPER;
    private static final Charset UTF8 = Charset.forName("UTF8");

    private static final long DEFAULT_RETRY_DELAY = 1000;
    private static final long KIB = 1024;

    private final List<UnexpectedReturnCodeHandler> handlers;
    protected final CloseableHttpClient httpClient;

    private final Map<Integer, Integer> retryCounts = new TreeMap<>();
    private final Map<Integer, Long> retryDelays = new TreeMap<>();

    private final ErrorReporter errorReporter;


    static
    {
        OBJECT_MAPPER = new ObjectMapper();
    }

    public RestHttpClient(ErrorReporter errorReporterRef)
    {
        this(errorReporterRef, 0, 0, 0);
    }

    public RestHttpClient(
        ErrorReporter errorReporterRef,
        int requestTimeoutMsRef,
        int connectTimeoutMsRef,
        int socketTimeoutMsRef
    )
    {
        errorReporter = errorReporterRef;

        httpClient = HttpClientBuilder.create().setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectionRequestTimeout(requestTimeoutMsRef)
                .setConnectTimeout(connectTimeoutMsRef)
                .setSocketTimeout(socketTimeoutMsRef)
                .build()
        ).build();
        handlers = new ArrayList<>();
    }

    @Override
    public void addFailHandler(UnexpectedReturnCodeHandler handler)
    {
        handlers.add(handler);
    }

    @Override
    public void setRetryCountOnStatusCode(int statusCode, int retryCount)
    {
        retryCounts.put(statusCode, retryCount);
    }

    @Override
    public void setRetryDelayOnStatusCode(int statusCode, long retryDelay)
    {
        retryDelays.put(statusCode, retryDelay);
    }

    @Override
    public <T> RestResponse<T> execute(RestHttpRequest<T> request)
        throws IOException, StorageException
    {
        HttpUriRequest req;
        switch (request.op)
        {
            case GET:
                req = new HttpGet(request.restURL);
                break;
            case POST:
                req = new HttpPost(request.restURL);
                break;
            case PUT:
                req = new HttpPut(request.restURL);
                break;
            case PATCH:
                req = new HttpPatch(request.restURL);
                break;
            case DELETE:
                req = new HttpDelete(request.restURL);
                break;
            default:
                throw new ImplementationError("Unknown Rest operation: " + request.op);
        }
        // add data if possible and available
        if (req instanceof HttpEntityEnclosingRequest && request.payload != null && !request.payload.isEmpty())
        {
            ((HttpEntityEnclosingRequest) req).setEntity(
                new StringEntity(
                    request.payload,
                    ContentType.APPLICATION_JSON
                )
            );
        }
        if (request.httpHeaders != null)
        {
            for (Entry<String, String> entry : request.httpHeaders.entrySet())
            {
                req.addHeader(entry.getKey(), entry.getValue());
            }
        }

        RestHttpResponse<T> restResponse = null;
        int attemptNumber = 0;
        boolean retry;
        do
        {
            retry = false;
            try (CloseableHttpResponse response = httpClient.execute(req);)
            {
                byte[] jsonData = readContent(response);
                Map<String, String> headers = new HashMap<>();
                Header[] msgHeaders = response.getAllHeaders();
                for (Header header : msgHeaders)
                {
                    headers.put(header.getName(), header.getValue());
                }

                T respObj;
                if (jsonData.length > 0 && request.responseClass != null)
                {
                    if (request.responseClass == String.class)
                    {
                        respObj = (T) new String(jsonData, UTF8);
                    }
                    else
                    {
                        /*
                         * Technically JSON allows duplicate keys, even with different value types.
                         * In some cases we need to get rid of this. (sorry for this ugly hack)
                         */
                        if (request.responseClass.isArray())
                        {
                            Map<String, Object>[] obj = OBJECT_MAPPER.readValue(
                                jsonData,
                                new TypeReference<Map<String, Object>[]>()
                                {
                                }
                            );
                            respObj = OBJECT_MAPPER
                                .readValue(OBJECT_MAPPER.writeValueAsString(obj), request.responseClass);
                        }
                        else
                        {
                            Map<String, Object> obj = OBJECT_MAPPER.readValue(
                                jsonData,
                                new TypeReference<Map<String, Object>>()
                                {
                                }
                            );
                            respObj = OBJECT_MAPPER
                                .readValue(OBJECT_MAPPER.writeValueAsString(obj), request.responseClass);
                        }
                    }
                }
                else
                {
                    respObj = null;
                }

                int statusCode = response.getStatusLine().getStatusCode();

                restResponse = new RestHttpResponse<>(
                    request,
                    statusCode,
                    headers,
                    respObj
                );
                if (!request.expectedRcs.contains(statusCode))
                {
                    Integer retryCount = retryCounts.get(statusCode);
                    Long retryDelay = retryDelays.get(statusCode);

                    if (retryCount != null && retryDelay == null)
                    {
                        errorReporter.logWarning(
                            "Status code %d has configured a retry count of %d, but has no retry delay defined. " +
                                "Defaulting to %d",
                            statusCode,
                            retryCount,
                            DEFAULT_RETRY_DELAY
                        );
                        retryDelay = DEFAULT_RETRY_DELAY;
                    }

                    if (retryCount == null || attemptNumber++ >= retryCount)
                    {
                        for (UnexpectedReturnCodeHandler<T> handler : handlers)
                        {
                            handler.handle(restResponse);
                        }
                        throw new StorageException(
                            "Unexpected status code",
                            "A REST call returned the unexpected status code " + restResponse.statusCode,
                            null,
                            null,
                            restResponse.toString()
                        );
                    }
                    else
                    {
                        retry = true;
                        errorReporter.logInfo(
                            "Request returned %d. Attempt count: %d / %d. Waiting %dms before next attempt",
                            statusCode,
                            attemptNumber,
                            retryCount,
                            retryDelay
                        );
                        try
                        {
                            Thread.sleep(retryDelay);
                        }
                        catch (InterruptedException exc)
                        {
                            throw new StorageException("Retry delay interrupted", exc);
                        }
                    }
                }
            }
        }
        while (retry);

        return restResponse;
    }

    private byte[] readContent(HttpResponse response) throws IOException
    {
        byte[] result;
        if (response == null || response.getEntity() == null)
        {
            result = new byte[0];
        }
        else
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[(int) KIB];
            InputStream inputStream = response.getEntity().getContent();
            int len = inputStream.read(buffer);
            while (len != -1)
            {
                baos.write(buffer, 0, len);
                len = inputStream.read(buffer);
            }
            inputStream.close();
            result = baos.toByteArray();
        }
        return result;
    }

    private class RestHttpResponse<T> implements RestResponse<T>
    {
        private final int statusCode;
        private final @Nullable T response;
        private final Map<String, String> headers;
        private final RestHttpRequest<T> request;

        RestHttpResponse(
            RestHttpRequest<T> requestRef,
            int statusCodeRef,
            Map<String, String> headersRef,
            @Nullable T responseRef
        )
        {
            request = requestRef;
            statusCode = statusCodeRef;
            headers = headersRef;
            response = responseRef;
        }

        @Override
        public @Nullable T getData()
        {
            return response;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        public Map<String, String> getHeaders()
        {
            return headers;
        }

        @Override
        public VlmProviderObject<Resource> getVolumeData()
        {
            return request.vlmData;
        }

        @Override
        public String toString()
        {
            return toString(request.expectedRcs);
        }

        @Override
        public String toString(Integer... excludeExpectedRcs)
        {
           ArrayList<Integer> list = new ArrayList<>(request.expectedRcs);
           list.removeAll(Arrays.asList(excludeExpectedRcs));
           return toString(list);
        }

        private String toString(Collection<Integer> shownExpectedRcs)
        {
            String jsonResponse;
            try
            {
                jsonResponse = JSON.std.asString(response);
            }
            catch (IOException exc)
            {
                jsonResponse = exc.getLocalizedMessage();
            }
            return String.format(
                "Request%n   %-8s %s%n   %-8s %s%n   %-8s %s%n   %-8s %s%n" +
                "Response%n   %-23s %d%n   %-23s %s%n   %-23s %s%n   %-23s %s",
                "Type", request.op,
                "URL", request.restURL,
                "Headers", request.httpHeaders,
                "Data", request.payload,
                "Status code", statusCode,
                "Expected status code(s)", shownExpectedRcs.toString(),
                "HEaders", headers,
                "Data", jsonResponse
            );
        }
    }
}
