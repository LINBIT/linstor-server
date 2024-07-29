package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestBaseResponse
{
    public @Nullable ExosStatus[] status;

    @Deprecated(forRemoval = true)
    public static class ExosStatus
    {
        public static final int STATUS_SUCCESS = 0;
        public static final int STATUS_ERROR = 1;
        public static final int STATUS_INFO = 2;

        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("response-type")
        public @Nullable String responseType;

        @JsonProperty("response-type-numeric")
        public long responseTypeNumeric;

        @JsonProperty("response")
        public @Nullable String response;

        @JsonProperty("return-code")
        public long returnCode;

        @JsonProperty("component-id")
        public @Nullable String componentId;

        @JsonProperty("time-stamp")
        public @Nullable String timestamp;

        @JsonProperty("time-stamp-numeric")
        public long timestampNumeric;
    }
}
