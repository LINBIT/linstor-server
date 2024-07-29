package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestMaps extends ExosRestBaseResponse
{
    @JsonProperty("volume-group-view")
    public @Nullable ExosVolumeGroupView[] volumeGroupView;

    @JsonProperty("volume-view")
    public @Nullable ExosVolumeView[] volumeView;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosVolumeGroupView
    {

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosVolumeView
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("volume-serial")
        public @Nullable String volumeSerial;

        @JsonProperty("volume-name")
        public @Nullable String volumeName;

        @JsonProperty("volume-view-mapping")
        public @Nullable ExosVolumeViewMapping[] volumeViewMappings;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public class ExosVolumeViewMapping
    {
        @JsonProperty("object-name")
        public @Nullable String objectName;

        @JsonProperty("durable-id")
        public @Nullable String durableId;

        @JsonProperty("parent-id")
        public @Nullable String parentId;

        @JsonProperty("mapped-id")
        public @Nullable String mappedId;

        @JsonProperty("ports")
        public @Nullable String ports;

        @JsonProperty("lun")
        public @Nullable String lun;

        @JsonProperty("access")
        public @Nullable String access;

        @JsonProperty("access-numeric")
        public long accessNumeric;

        @JsonProperty("identifier")
        public @Nullable String identifier;

        @JsonProperty("nickname")
        public @Nullable String nickname;

        @JsonProperty("host-profile")
        public @Nullable String hostProfile;

        @JsonProperty("host-profile-numeric")
        public long hostProfileNumeric;
    }

}
