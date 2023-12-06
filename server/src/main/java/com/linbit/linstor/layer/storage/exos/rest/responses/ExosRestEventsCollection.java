package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestEventsCollection extends ExosRestBaseResponse
{
    @JsonProperty("events")
    public ExosRestEvent[] events;

    public static class ExosRestEvent
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("time-stamp")
        public String timeStamp;

        @JsonProperty("time-stamp-numeric")
        public long timeStampNumeric;

        @JsonProperty("event-code")
        public String eventCode;

        @JsonProperty("event-id")
        public String eventId;

        @JsonProperty("model")
        public String model;

        @JsonProperty("serial-number")
        public String serialNumber;

        @JsonProperty("controller")
        public String controller;

        @JsonProperty("controller-numeric")
        public long controllerNumeric;

        @JsonProperty("severity")
        public String severity;

        @JsonProperty("severity-numeric")
        public long severityNumeric;

        @JsonProperty("message")
        public String message;

        @JsonProperty("additional-information")
        public String additionalInformation;

        @JsonProperty("recommended-action")
        public String recommendedAction;
    }
}
