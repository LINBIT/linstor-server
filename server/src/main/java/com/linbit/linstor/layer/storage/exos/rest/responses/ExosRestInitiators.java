package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestInitiators extends ExosRestBaseResponse
{
    @JsonProperty("initiator")
    public ExosRestInitiator[] initiator;

    public static class ExosRestInitiator
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("nickname")
        public String nickname;

        @JsonProperty("discovered")
        public String discovered;

        @JsonProperty("mapped")
        public String mapped;

        @JsonProperty("profile")
        public String profile;

        @JsonProperty("profile-numeric")
        public long profileNumeric;

        @JsonProperty("host-bus-type")
        public String hostBusType;

        @JsonProperty("host-bus-type-numeric")
        public long hostBusTypeNumeric;

        @JsonProperty("id")
        public String id;

        @JsonProperty("host-id")
        public String hostId;

        @JsonProperty("host-key")
        public String hostKey;

        @JsonProperty("host-port-bits-a")
        public long hostPortBitsA;

        @JsonProperty("host-port-bits-b")
        public long hostPortBitsB;
    }
}
