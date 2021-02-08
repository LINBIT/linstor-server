package com.linbit.linstor.storage.utils;

import com.linbit.utils.Base64;

import java.util.HashMap;
import java.util.Map;

public class HttpHeader
{
    public static final int HTTP_OK = 200;
    public static final int HTTP_CREATED = 201;
    public static final int HTTP_ACCEPTED = 202;
    public static final int HTTP_NO_CONTENT = 204;

    public static final int HTTP_BAD_REQUEST = 400;
    public static final int HTTP_FORBIDDEN = 403;
    public static final int HTTP_NOT_FOUND = 404;
    public static final int HTTP_CONFLICT = 409;
    public static final int HTTP_PRECONDITION_FAILED = 412;
    public static final int HTTP_PRECONDITION_REQUIRED = 428;
    public static final int HTTP_TOO_MANY_REQUESTS = 429;

    public static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    public static final int HTTP_SERVICE_UNAVAILABLE = 503;

    public static final String LOCATION_KEY = "Location";
    public static final String CONTENT_TYPE_KEY = "Content-Type";
    public static final String ACCEPT_KEY = "Accept";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_TEXT = "application/text";
    public static final String AUTH_KEY = "Authorization";

    public static final String ETAG_KEY = "Etag";
    public static final String IF_MATCH = "If-Match";

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public static class Builder
    {

        private Map<String, String> map;

        public Builder()
        {
            map = new HashMap<>();
        }

        public Builder put(String key, String value)
        {
            map.put(key, value);
            return this;
        }

        public Map<String, String> build()
        {
            return map;
        }

        public Builder putAll(Map<String, String> headers)
        {
            map.putAll(headers);
            return this;
        }

        public Builder setJsonContentType()
        {
            map.put(CONTENT_TYPE_KEY, APPLICATION_JSON);
            return this;
        }

        public Builder setPlainContentType()
        {
            map.put(CONTENT_TYPE_KEY, APPLICATION_TEXT);
            return this;
        }

        public Builder noContentType()
        {
            map.remove(CONTENT_TYPE_KEY);
            return this;
        }

        public Builder setAuth(String userName, String userPw)
        {
            map.put(
                AUTH_KEY,
                "Basic " + Base64.encode((userName + ":" + userPw).getBytes())
            );
            return this;
        }

        public Builder setAcceptsJson()
        {
            map.put(ACCEPT_KEY, APPLICATION_JSON);
            return this;
        }

        public Builder setAcceptsText()
        {
            map.put(ACCEPT_KEY, APPLICATION_TEXT);
            return this;
        }
    }

    private HttpHeader()
    {
    }
}
