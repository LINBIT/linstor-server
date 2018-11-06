package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.StorageException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.fasterxml.jackson.jr.ob.JSON;

public class RestHttpClient implements RestClient
{
    private static final long DEFAULT_RETRY_DELAY = 1000;
    private static final long KIB = 1024;

    private final List<UnexpectedReturnCodeHandler> handlers;
    protected final HttpClient httpClient;

    private final Map<Integer, Integer> retryCounts = new TreeMap<>();
    private final Map<Integer, Long> retryDelays = new TreeMap<>();

    private final ErrorReporter errorReporter;

    public RestHttpClient(ErrorReporter errorReporterRef)
    {
        errorReporter = errorReporterRef;
        httpClient = HttpClientBuilder.create().build();
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
    public RestResponse<Map<String, Object>> execute(RestHttpRequest request)
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
        if (req instanceof HttpEntityEnclosingRequest && request.payload != null && !request.payload.equals(""))
        {
            ((HttpEntityEnclosingRequest) req).setEntity(
                new StringEntity(
                    request.payload,
                    ContentType.APPLICATION_JSON
                )
            );
        }
        for (Entry<String, String> entry : request.httpHeaders.entrySet())
        {
            req.addHeader(entry.getKey(), entry.getValue());
        }

        RestHttpResponse restResponse = null;
        int attemptNumber = 0;
        boolean retry;
        do
        {
            retry = false;
            HttpResponse response = httpClient.execute(req);

            byte[] responseContent = readContent(response);
            Map<String, String> sfHeaders = new HashMap<>();
            Header[] msgHeaders = response.getAllHeaders();
            for (Header header : msgHeaders)
            {
                sfHeaders.put(header.getName(), header.getValue());
            }

            Map<String, Object> respRoot;
            if (responseContent.length > 0)
            {
                respRoot = JSON.std.mapFrom(responseContent);
            }
            else
            {
                respRoot = Collections.emptyMap();
            }

            int statusCode = response.getStatusLine().getStatusCode();

            restResponse = new RestHttpResponse(
                request,
                statusCode,
                sfHeaders,
                respRoot
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
                    for (UnexpectedReturnCodeHandler handler : handlers)
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

    private class RestHttpResponse implements RestResponse<Map<String, Object>>
    {
        private final int statusCode;
        private final Map<String, Object> respRoot;
        private final Map<String, String> headers;
        private final RestHttpRequest request;

        RestHttpResponse(
            RestHttpRequest requestRef,
            int statusCodeRef,
            Map<String, String> headersRef,
            Map<String, Object> respRootRef
        )
        {
            request = requestRef;
            statusCode = statusCodeRef;
            headers = headersRef;
            respRoot = respRootRef;
        }

        @Override
        public Map<String, Object> getData()
        {
            return respRoot;
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
        public String getLinstorVlmId()
        {
            return request.linstorVlmId;
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
                "Data", respRoot
            );
        }
    }
}
