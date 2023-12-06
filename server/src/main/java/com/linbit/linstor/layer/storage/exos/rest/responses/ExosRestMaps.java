package com.linbit.linstor.layer.storage.exos.rest.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated(forRemoval = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExosRestMaps extends ExosRestBaseResponse
{
    @JsonProperty("volume-group-view")
    public ExosVolumeGroupView[] volumeGroupView;

    @JsonProperty("volume-view")
    public ExosVolumeView[] volumeView;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosVolumeGroupView
    {

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExosVolumeView
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("volume-serial")
        public String volumeSerial;

        @JsonProperty("volume-name")
        public String volumeName;

        @JsonProperty("volume-view-mapping")
        public ExosVolumeViewMapping[] volumeViewMappings;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public class ExosVolumeViewMapping
    {
        @JsonProperty("object-name")
        public String objectName;

        @JsonProperty("durable-id")
        public String durableId;

        @JsonProperty("parent-id")
        public String parentId;

        @JsonProperty("mapped-id")
        public String mappedId;

        @JsonProperty("ports")
        public String ports;

        @JsonProperty("lun")
        public String lun;

        @JsonProperty("access")
        public String access;

        @JsonProperty("access-numeric")
        public long accessNumeric;

        @JsonProperty("identifier")
        public String identifier;

        @JsonProperty("nickname")
        public String nickname;

        @JsonProperty("host-profile")
        public String hostProfile;

        @JsonProperty("host-profile-numeric")
        public long hostProfileNumeric;
    }

}
