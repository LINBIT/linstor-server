package com.linbit.linstor.api.rest.v1.serializer;

import com.linbit.linstor.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;

public class JsonSpaceTracking
{
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class SpaceReport
    {
        public @Nullable String reportText;
    }
}
