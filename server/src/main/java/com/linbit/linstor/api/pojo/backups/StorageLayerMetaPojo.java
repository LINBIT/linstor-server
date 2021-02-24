package com.linbit.linstor.api.pojo.backups;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class StorageLayerMetaPojo
{
    private final Map<Integer, StorageLayerVlmMetaPojo> vlmsMap;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public StorageLayerMetaPojo(@JsonProperty("vlmsMap") Map<Integer, StorageLayerVlmMetaPojo> vlmsMapRef)
    {
        vlmsMap = vlmsMapRef;
    }

    public Map<Integer, StorageLayerVlmMetaPojo> getVlmsMap()
    {
        return vlmsMap;
    }


    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class StorageLayerVlmMetaPojo
    {
        private final int vlmNr;
        private final String storPoolName;
        private final String storType;

        @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
        public StorageLayerVlmMetaPojo(
            @JsonProperty("vlmNr") int vlmNrRef,
            @JsonProperty("storPoolName") String storPoolNameRef,
            @JsonProperty("storType") String storTypeRef
        )
        {
            vlmNr = vlmNrRef;
            storPoolName = storPoolNameRef;
            storType = storTypeRef;
        }

        public int getVlmNr()
        {
            return vlmNr;
        }

        public String getStorPoolName()
        {
            return storPoolName;
        }

        public String getStorType()
        {
            return storType;
        }
    }
}
