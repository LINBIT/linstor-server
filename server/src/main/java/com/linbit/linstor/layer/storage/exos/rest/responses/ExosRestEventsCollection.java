package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestEventsCollection extends ExosRestBaseResponse
{
    @JsonProperty("events")
    public @Nullable ExosRestEvent[] events;

    public static class ExosRestEvent
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("time-stamp")
        public @Nullable String timeStamp;

        @JsonProperty("time-stamp-numeric")
        public long timeStampNumeric;

        @JsonProperty("event-code")
        public @Nullable String eventCode;

        @JsonProperty("event-id")
        public @Nullable String eventId;

        @JsonProperty("model")
        public @Nullable String model;

        @JsonProperty("serial-number")
        public @Nullable String serialNumber;

        @JsonProperty("controller")
        public @Nullable String controller;

        @JsonProperty("controller-numeric")
        public long controllerNumeric;

        @JsonProperty("severity")
        public @Nullable String severity;

        @JsonProperty("severity-numeric")
        public long severityNumeric;

        @JsonProperty("message")
        public @Nullable String message;

        @JsonProperty("additional-information")
        public @Nullable String additionalInformation;

        @JsonProperty("recommended-action")
        public @Nullable String recommendedAction;
    }
}
