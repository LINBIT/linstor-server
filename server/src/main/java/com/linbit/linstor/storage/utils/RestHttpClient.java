package com.linbit.linstor.storage.utils;

import com.linbit.ImplementationError;
import com.linbit.linstor.storage.StorageException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    private static final long KIB = 1024;

    protected HttpClient httpClient;

    public RestHttpClient()
    {
        httpClient = HttpClientBuilder.create().build();
    }

    @Override
    public RestResponse<Map<String, Object>> execute(
        RestOp op,
        String restURL,
        Map<String, String> httpHeaders,
        String jsonString,
        List<Integer> expectedRcs
    )
        throws IOException, StorageException
    {
        HttpUriRequest req;
        String payload;
        switch (op)
        {
            case GET:
                req = new HttpGet(restURL);
                break;
            case POST:
                req = new HttpPost(restURL);
                break;
            case PUT:
                req = new HttpPut(restURL);
                break;
            case PATCH:
                req = new HttpPatch(restURL);
                break;
            case DELETE:
                req = new HttpDelete(restURL);
                break;
            default:
                throw new ImplementationError("Unknown Rest operation: " + op);
        }
        // add data if possible and available
        if (req instanceof HttpEntityEnclosingRequest && jsonString != null && !jsonString.equals(""))
        {
            ((HttpEntityEnclosingRequest) req).setEntity(
                new StringEntity(
                    jsonString,
                    ContentType.APPLICATION_JSON
                )
            );
            payload = jsonString;
        }
        else
        {
            payload = "";
        }
        for (Entry<String, String> entry : httpHeaders.entrySet())
        {
            req.addHeader(entry.getKey(), entry.getValue());
        }

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
        if (!expectedRcs.contains(statusCode))
        {
            throw new StorageException(
                "Unexpected status code",
                "A REST call returned the unexpected status code " + statusCode,
                null,
                null,
                String.format(
                    "Request%n   %-8s %s%n   %-8s %s%n   %-8s %s%n   %-8s %s%n" +
                    "Response%n   %-23s %d%n   %-23s %s%n   %-23s %s",
                    "Type", op.name(),
                    "URL", restURL,
                    "Headers", httpHeaders,
                    "Data", payload,
                    "Status code", statusCode,
                    "Expected status code(s)", expectedRcs.toString(),
                    "Data", respRoot
                )
            );
        }

        return new RestHttpResponse(statusCode, sfHeaders, respRoot);
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
        private int statusCode;
        private Map<String, Object> respRoot;
        private Map<String, String> headers;

        RestHttpResponse(int statusCodeRef, Map<String, String> headersRef, Map<String, Object> respRootRef)
        {
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
    }
}
