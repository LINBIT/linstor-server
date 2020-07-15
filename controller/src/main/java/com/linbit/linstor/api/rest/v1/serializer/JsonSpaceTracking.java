package com.linbit.linstor.api.rest.v1.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;

public class JsonSpaceTracking
{
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SpaceReport
    {
        public String reportText;
    }
}
