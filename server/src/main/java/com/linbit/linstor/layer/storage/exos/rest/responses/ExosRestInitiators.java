package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
public class ExosRestInitiators extends ExosRestBaseResponse
{
    @JsonProperty("initiator")
    public @Nullable ExosRestInitiator[] initiator;

    public static class ExosRestInitiator
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("nickname")
        public @Nullable String nickname;

        @JsonProperty("discovered")
        public @Nullable String discovered;

        @JsonProperty("mapped")
        public @Nullable String mapped;

        @JsonProperty("profile")
        public @Nullable String profile;

        @JsonProperty("profile-numeric")
        public long profileNumeric;

        @JsonProperty("host-bus-type")
        public @Nullable String hostBusType;

        @JsonProperty("host-bus-type-numeric")
        public long hostBusTypeNumeric;

        @JsonProperty("id")
        public @Nullable String id;

        @JsonProperty("host-id")
        public @Nullable String hostId;

        @JsonProperty("host-key")
        public @Nullable String hostKey;

        @JsonProperty("host-port-bits-a")
        public long hostPortBitsA;

        @JsonProperty("host-port-bits-b")
        public long hostPortBitsB;
    }
}
